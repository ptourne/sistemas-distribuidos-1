package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os/exec"
	"strings"
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

func (cm *ChaosMonkey) killContainer(containerID string, containerName string) error {
	log.Printf("🔥 Killing container: %s (%s)", containerName, containerID[:12])

	cmd := exec.Command("docker", "kill", containerID)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to kill container %s: %w", containerName, err)
	}

	log.Printf("💀 Container %s killed successfully", containerName)
	return nil
}

func (cm *ChaosMonkey) run() {
	log.Println("🐒 Chaos Monkey starting...")
	log.Printf("📋 Excluded containers: %v", cm.excludeList)
	log.Println("⚠️  Will preserve at least one monitor container")
	log.Println("⏰ Killing one container every second...")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	consecutiveErrors := 0
	maxConsecutiveErrors := 10

	for range ticker.C {
		containers, err := cm.getRunningContainers()
		if err != nil {
			consecutiveErrors++
			if consecutiveErrors <= 3 {
				log.Printf("❌ Error getting running containers: %v", err)
			} else if consecutiveErrors == maxConsecutiveErrors {
				log.Printf("❌ Too many consecutive errors (%d). Docker daemon might be down. Stopping chaos monkey.", maxConsecutiveErrors)
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
		if err := cm.killContainer(victim.ID, name); err != nil {
			log.Printf("❌ Error killing container: %v", err)
		}
	}
}

func main() {

	chaosMonkey := NewChaosMonkey()
	chaosMonkey.run()
}
