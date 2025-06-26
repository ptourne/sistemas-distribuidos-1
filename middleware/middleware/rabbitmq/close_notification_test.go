package rabbitmq

import (
	"fmt"
	"testing"
)

func TestCloseNotificationEncode(t *testing.T) {
	// Create a CloseNotification instance
	notification := &CloseNotification{
		notificationType: closeNotificationFinishCidDone,
		idWorker:         "2",
	}

	// Encode the notification
	encoded, err := notification.Encode()
	if err != nil {
		t.Fatalf("Failed to encode CloseNotification: %v", err)
	}

	// Print the encoded bytes as string
	fmt.Printf("Encoded CloseNotification as string: %s\n", string(encoded))
	fmt.Printf("Encoded CloseNotification as hex: %x\n", encoded)
	fmt.Printf("Encoded CloseNotification length: %d bytes\n", len(encoded))

	// Decode it back to verify
	decoded, err := (&CloseNotification{}).Decode(encoded)
	if err != nil {
		t.Fatalf("Failed to decode CloseNotification: %v", err)
	}

	fmt.Printf("Decoded notification type: %v\n", decoded.notificationType)
	fmt.Printf("Decoded idWorker: %s\n", decoded.idWorker)

	// Verify the decoded values match the original
	if decoded.notificationType != notification.notificationType {
		t.Errorf("Notification type mismatch: expected %v, got %v",
			notification.notificationType, decoded.notificationType)
	}
	if decoded.idWorker != notification.idWorker {
		t.Errorf("ID worker mismatch: expected %s, got %s",
			notification.idWorker, decoded.idWorker)
	}
}

func TestCloseNotificationEncodeExample(t *testing.T) {
	// Example with a specific worker ID
	r := struct {
		idWorker string
	}{
		idWorker: "joiner-credits-0",
	}

	notification := &CloseNotification{
		notificationType: closeNotificationFinishCidDone,
		idWorker:         r.idWorker,
	}

	// Encode the notification
	encoded, err := notification.Encode()
	if err != nil {
		t.Fatalf("Failed to encode CloseNotification: %v", err)
	}

	fmt.Printf("Example CloseNotification:\n")
	fmt.Printf("  Original: %+v\n", notification)
	fmt.Printf("  Encoded as string: %s\n", string(encoded))
	fmt.Printf("  Encoded as hex: %x\n", encoded)
	fmt.Printf("  Encoded length: %d bytes\n", len(encoded))
}
