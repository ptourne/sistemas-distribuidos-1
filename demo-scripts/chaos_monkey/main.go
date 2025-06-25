package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type Container struct {
	ID    string `json:"ID"`
	Names string `json:"Names"`
	State string `json:"State"`
}

type ChaosMonkey struct {
	excludeList []string
}

func NewChaosMonkey() *ChaosMonkey {
	// Containers that should never be killed
	excludeList := []string{
		"client",
		"rabbitmq",
		"endpoint",
	}

	return &ChaosMonkey{
		excludeList: excludeList,
	}
}

func (cm *ChaosMonkey) getRunningContainers() ([]Container, error) {
	cmd := exec.Command("docker", "ps", "--format", "json")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}

	var containers []Container
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")

	for _, line := range lines {
		if line == "" {
			continue
		}

		var container Container
		if err := json.Unmarshal([]byte(line), &container); err != nil {
			log.Printf("Warning: failed to parse container JSON: %v\n%s", err, line)
			continue
		}

		// Only include running containers
		if container.State == "running" {
			containers = append(containers, container)
		}
	}

	return containers, nil
}

func (cm *ChaosMonkey) isExcluded(containerName string) bool {
	for _, excluded := range cm.excludeList {
		if strings.Contains(containerName, excluded) {
			return true
		}
	}
	return false
}

func (cm *ChaosMonkey) countMonitors(containers []Container) int {
	count := 0
	for _, container := range containers {
		if len(container.Names) > 0 {
			name := strings.TrimPrefix(container.Names, "/")
			if strings.Contains(name, "monitor") {
				count++
			}
		}
	}
	return count
}

func (cm *ChaosMonkey) selectVictim(containers []Container) *Container {
	var candidates []Container
	monitorCount := cm.countMonitors(containers)

	for _, container := range containers {
		if len(container.Names) == 0 {
			continue
		}

		name := strings.TrimPrefix(container.Names, "/")

		// Skip excluded containers
		if cm.isExcluded(name) {
			continue
		}

		// If there's only one monitor running, exclude it from selection
		if monitorCount <= 1 && strings.Contains(name, "monitor") {
			log.Printf("Skipping monitor %s - only %d monitor(s) running", name, monitorCount)
			continue
		}

		candidates = append(candidates, container)
	}

	if len(candidates) == 0 {
		return nil
	}

	// Select random victim
	victim := candidates[rand.Intn(len(candidates))]
	return &victim
}

func (cm *ChaosMonkey) killAllEligible() error {
	containers, err := cm.getRunningContainers()
	if err != nil {
		return fmt.Errorf("failed to get containers: %w", err)
	}

	if len(containers) == 0 {
		log.Println("ℹ️  No running containers found")
		return nil
	}

	// Get all eligible victims
	var victims []Container
	monitorCount := cm.countMonitors(containers)

	for _, container := range containers {
		if len(container.Names) == 0 {
			continue
		}

		name := strings.TrimPrefix(container.Names, "/")

		// Skip excluded containers
		if cm.isExcluded(name) {
			continue
		}

		// If there's only one monitor running, exclude it from selection
		if monitorCount <= 1 && strings.Contains(name, "monitor") {
			log.Printf("Skipping monitor %s - only %d monitor(s) running", name, monitorCount)
			continue
		}

		victims = append(victims, container)
	}

	if len(victims) == 0 {
		log.Println("ℹ️  No eligible victims found (all containers are protected)")
		return nil
	}

	log.Printf("🔥 Found %d eligible victims. Killing all...", len(victims))

	killed := 0
	handlers := make([]chan error, len(victims))
	for i, victim := range victims {
		name := strings.TrimPrefix(victim.Names, "/")
		handlers[i] = killContainer(victim.ID, name)
	}

	for i, handler := range handlers {
		if err := <-handler; err != nil {
			log.Printf("❌ Error killing container %s: %v", victims[i].Names, err)
		} else {
			killed++
		}
		// Small delay between kills to avoid overwhelming the system
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("💀 Successfully killed %d out of %d containers", killed, len(victims))
	return nil
}

func (cm *ChaosMonkey) killOnce() error {
	containers, err := cm.getRunningContainers()
	if err != nil {
		return fmt.Errorf("failed to get containers: %w", err)
	}

	if len(containers) == 0 {
		log.Println("ℹ️  No running containers found")
		return nil
	}

	victim := cm.selectVictim(containers)
	if victim == nil {
		log.Println("ℹ️  No suitable victim found (all containers are protected)")
		return nil
	}

	name := strings.TrimPrefix(victim.Names, "/")
	return <-killContainer(victim.ID, name)
}

func (cm *ChaosMonkey) showStatus() {
	containers, err := cm.getRunningContainers()
	if err != nil {
		log.Printf("❌ Error getting containers: %v", err)
		return
	}

	if len(containers) == 0 {
		log.Println("ℹ️  No running containers found")
		return
	}

	log.Printf("📊 Container Status:")
	log.Printf("   Total running containers: %d", len(containers))

	protected := 0
	monitors := 0
	eligible := 0

	monitorCount := cm.countMonitors(containers)

	for _, container := range containers {
		if len(container.Names) == 0 {
			continue
		}

		name := strings.TrimPrefix(container.Names, "/")

		if cm.isExcluded(name) {
			protected++
		} else if strings.Contains(name, "monitor") && monitorCount <= 1 {
			monitors++
		} else if strings.Contains(name, "monitor") {
			eligible++
			monitors++
		} else {
			eligible++
		}
	}

	log.Printf("   Protected containers: %d", protected)
	log.Printf("   Monitor containers: %d", monitors)
	log.Printf("   Eligible for chaos: %d", eligible)
}

func killContainer(containerID string, containerName string) chan error {
	res := make(chan error)
	go func() {
		log.Printf("🔥 Killing container: %s (%s)", containerName, containerID[:12])

		cmd := exec.Command("docker", "kill", containerID)
		if err := cmd.Run(); err != nil {
			res <- fmt.Errorf("failed to kill container %s: %w", containerName, err)
			return
		}

		log.Printf("💀 Container %s killed successfully", containerName)
		close(res)
	}()
	return res
}

func (cm *ChaosMonkey) showMenu() {
	fmt.Println("\n🐒 ===== CHAOS MONKEY CONTROL CENTER =====")
	fmt.Println("1. 👀 Show container status")
	fmt.Println("2. ⚡ Kill one random container")
	fmt.Println("3. 💥 Kill ALL eligible containers")
	fmt.Println("4. 🔄 Start auto-killer (1 sec interval, Ctrl+C to stop)")
	fmt.Println("5. ❌ Exit")
	fmt.Print("Choose an option (1-5): ")
}

func (cm *ChaosMonkey) getUserInput() string {
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	return strings.TrimSpace(input)
}

func (cm *ChaosMonkey) runInteractive() {
	fmt.Println("🐒 Welcome to Chaos Monkey Interactive!")
	fmt.Printf("📋 Protected containers: %v\n", cm.excludeList)
	fmt.Println("⚠️  Monitor containers are protected if only 1 is running")

	for {
		cm.showMenu()
		choice := cm.getUserInput()

		switch choice {
		case "1":
			fmt.Println("\n📊 Checking container status...")
			cm.showStatus()

		case "2":
			fmt.Println("\n⚡ Killing one random container...")
			if err := cm.killOnce(); err != nil {
				log.Printf("❌ Error: %v", err)
			}

		case "3":
			fmt.Println("\n💥 Killing all eligible containers...")
			if err := cm.killAllEligible(); err != nil {
				log.Printf("❌ Error: %v", err)
			}

		case "4":
			fmt.Println("\n🔄 Starting auto-killer mode (1 second interval)...")
			fmt.Println("⚠️  Press Ctrl+C to return to menu")
			cm.runAutoKiller()

		case "5":
			fmt.Println("\n👋 Goodbye! Chaos Monkey shutting down...")
			return

		default:
			fmt.Println("\n❌ Invalid option. Please choose 1-5.")
		}

		// Only pause after showing status, not after killing
		if choice == "1" {
			fmt.Println("\nPress Enter to continue...")
			cm.getUserInput()
		}
	}
}

func (cm *ChaosMonkey) runAutoKiller() {
	// Set up signal channel to handle Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	consecutiveErrors := 0
	maxConsecutiveErrors := 10

	fmt.Println("🐒 Auto-killer started! Killing one container every second...")

	for {
		select {
		case <-sigChan:
			fmt.Println("\n🛑 Auto-killer stopped by user. Returning to menu...")
			signal.Reset(os.Interrupt, syscall.SIGTERM)
			return

		case <-ticker.C:
			containers, err := cm.getRunningContainers()
			if err != nil {
				consecutiveErrors++
				if consecutiveErrors <= 3 {
					log.Printf("❌ Error getting running containers: %v", err)
				} else if consecutiveErrors == maxConsecutiveErrors {
					log.Printf("❌ Too many consecutive errors (%d). Stopping auto-killer.", maxConsecutiveErrors)
					signal.Reset(os.Interrupt, syscall.SIGTERM)
					return
				}
				continue
			}

			// Reset error counter on successful operation
			consecutiveErrors = 0

			if len(containers) == 0 {
				log.Println("ℹ️  No running containers found")
				continue
			}

			victim := cm.selectVictim(containers)
			if victim == nil {
				log.Println("ℹ️  No suitable victim found (all containers are protected)")
				continue
			}

			name := strings.TrimPrefix(victim.Names, "/")
			if err := <-killContainer(victim.ID, name); err != nil {
				log.Printf("❌ Error killing container: %v", err)
			}
		}
	}
}

func main() {
	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	chaosMonkey := NewChaosMonkey()
	chaosMonkey.runInteractive()
}
