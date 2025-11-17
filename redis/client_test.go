package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/m-lab/tcp-info/inetdiag"
	"github.com/m-lab/tcp-info/netlink"
	"github.com/redis/go-redis/v9"
)

// createTestClient creates a Redis client connected to an in-memory miniredis server
func createTestClient(t *testing.T) (*Client, *miniredis.Miniredis) {
	t.Helper()

	// Start miniredis server
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}

	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	ctx := context.Background()

	client := &Client{
		rdb: rdb,
		ctx: ctx,
	}

	return client, mr
}

// createDummyArchivalRecord creates a dummy ArchivalRecord for testing
func createDummyArchivalRecord(timestamp time.Time) *netlink.ArchivalRecord {
	// Create a minimal RawInetDiagMsg as a byte slice
	// We need at least 72 bytes for a valid InetDiagMsg (size of the struct)
	rawIDM := make(inetdiag.RawInetDiagMsg, 72)
	// Set some dummy values:
	rawIDM[0] = 2  // IDiagFamily = AF_INET
	rawIDM[1] = 1  // IDiagState = TCP_ESTABLISHED
	rawIDM[2] = 0  // IDiagTimer
	rawIDM[3] = 0  // IDiagRetrans

	// Create dummy attributes (simplified)
	attributes := [][]byte{
		nil,           // 0
		[]byte("test"), // 1
	}

	return &netlink.ArchivalRecord{
		Timestamp:  timestamp,
		RawIDM:     rawIDM,
		Attributes: attributes,
	}
}

func TestAppendTCPInfo(t *testing.T) {
	client, mr := createTestClient(t)
	defer mr.Close()
	defer client.Close()

	ctx := context.Background()
	uuid := "test-uuid-123"
	timestamp := time.Now()

	// Create dummy record
	record := createDummyArchivalRecord(timestamp)

	// Test appending
	err := client.AppendTCPInfo(ctx, uuid, record)
	if err != nil {
		t.Fatalf("AppendTCPInfo failed: %v", err)
	}

	// Verify data was written to Redis
	key := table1Prefix + uuid
	length, err := client.rdb.LLen(ctx, key).Result()
	if err != nil {
		t.Fatalf("Failed to get list length: %v", err)
	}

	if length != 1 {
		t.Errorf("Expected list length 1, got %d", length)
	}

	// Verify the data structure
	data, err := client.rdb.LIndex(ctx, key, 0).Result()
	if err != nil {
		t.Fatalf("Failed to get list item: %v", err)
	}

	var tcpInfo TCPInfo
	err = json.Unmarshal([]byte(data), &tcpInfo)
	if err != nil {
		t.Fatalf("Failed to unmarshal TCPInfo: %v", err)
	}

	if tcpInfo.UUID != uuid {
		t.Errorf("Expected UUID %s, got %s", uuid, tcpInfo.UUID)
	}

	if tcpInfo.Data == nil {
		t.Error("Expected Data to be non-nil")
	}
}

func TestAppendTCPInfo_NilRecord(t *testing.T) {
	client, mr := createTestClient(t)
	defer mr.Close()
	defer client.Close()

	ctx := context.Background()
	uuid := "test-uuid-456"

	// Test with nil record
	err := client.AppendTCPInfo(ctx, uuid, nil)
	if err == nil {
		t.Error("Expected error when appending nil record, got nil")
	}
}

func TestAppendTCPInfo_MultipleEntries(t *testing.T) {
	client, mr := createTestClient(t)
	defer mr.Close()
	defer client.Close()

	ctx := context.Background()
	uuid := "test-uuid-789"

	// Append multiple records
	numRecords := 5
	for i := 0; i < numRecords; i++ {
		timestamp := time.Now().Add(time.Duration(i) * time.Second)
		record := createDummyArchivalRecord(timestamp)

		err := client.AppendTCPInfo(ctx, uuid, record)
		if err != nil {
			t.Fatalf("AppendTCPInfo failed on iteration %d: %v", i, err)
		}
	}

	// Verify list length
	key := table1Prefix + uuid
	length, err := client.rdb.LLen(ctx, key).Result()
	if err != nil {
		t.Fatalf("Failed to get list length: %v", err)
	}

	if length != int64(numRecords) {
		t.Errorf("Expected list length %d, got %d", numRecords, length)
	}
}

func TestGetTCPInfoHistory(t *testing.T) {
	client, mr := createTestClient(t)
	defer mr.Close()
	defer client.Close()

	ctx := context.Background()
	uuid := "test-uuid-history"

	// Append some records first
	numRecords := 3
	timestamps := make([]time.Time, numRecords)
	for i := 0; i < numRecords; i++ {
		timestamps[i] = time.Now().Add(time.Duration(i) * time.Second)
		record := createDummyArchivalRecord(timestamps[i])

		err := client.AppendTCPInfo(ctx, uuid, record)
		if err != nil {
			t.Fatalf("AppendTCPInfo failed: %v", err)
		}
	}

	// Get history
	history, err := client.GetTCPInfoHistory(ctx, uuid)
	if err != nil {
		t.Fatalf("GetTCPInfoHistory failed: %v", err)
	}

	if len(history) != numRecords {
		t.Errorf("Expected %d records in history, got %d", numRecords, len(history))
	}

	// Verify each record has correct UUID
	for i, info := range history {
		if info.UUID != uuid {
			t.Errorf("Record %d: expected UUID %s, got %s", i, uuid, info.UUID)
		}
		if info.Data == nil {
			t.Errorf("Record %d: expected Data to be non-nil", i)
		}
	}
}

func TestGetTCPInfoHistory_EmptyList(t *testing.T) {
	client, mr := createTestClient(t)
	defer mr.Close()
	defer client.Close()

	ctx := context.Background()
	uuid := "test-uuid-empty"

	// Get history for non-existent UUID
	history, err := client.GetTCPInfoHistory(ctx, uuid)
	if err != nil {
		t.Fatalf("GetTCPInfoHistory failed: %v", err)
	}

	if len(history) != 0 {
		t.Errorf("Expected empty history, got %d records", len(history))
	}
}

func TestAppendTCPInfo_Trimming(t *testing.T) {
	client, mr := createTestClient(t)
	defer mr.Close()
	defer client.Close()

	ctx := context.Background()
	uuid := "test-uuid-trim"

	// Append more than 1000 records to test trimming
	numRecords := 1050
	for i := 0; i < numRecords; i++ {
		record := createDummyArchivalRecord(time.Now())
		err := client.AppendTCPInfo(ctx, uuid, record)
		if err != nil {
			t.Fatalf("AppendTCPInfo failed on iteration %d: %v", i, err)
		}
	}

	// Verify list is trimmed to 1000
	key := table1Prefix + uuid
	length, err := client.rdb.LLen(ctx, key).Result()
	if err != nil {
		t.Fatalf("Failed to get list length: %v", err)
	}

	if length != 1000 {
		t.Errorf("Expected list to be trimmed to 1000, got %d", length)
	}
}

func TestAppendTCPInfo_TTL(t *testing.T) {
	client, mr := createTestClient(t)
	defer mr.Close()
	defer client.Close()

	ctx := context.Background()
	uuid := "test-uuid-ttl"

	// Append a record
	record := createDummyArchivalRecord(time.Now())
	err := client.AppendTCPInfo(ctx, uuid, record)
	if err != nil {
		t.Fatalf("AppendTCPInfo failed: %v", err)
	}

	// Check TTL is set
	key := table1Prefix + uuid
	ttl, err := client.rdb.TTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("Failed to get TTL: %v", err)
	}

	// TTL should be close to 24 hours (allowing some variance for execution time)
	expectedTTL := 24 * time.Hour
	if ttl < expectedTTL-time.Minute || ttl > expectedTTL {
		t.Errorf("Expected TTL close to %v, got %v", expectedTTL, ttl)
	}
}

func TestAppendTCPInfo_DataStructure(t *testing.T) {
	client, mr := createTestClient(t)
	defer mr.Close()
	defer client.Close()

	ctx := context.Background()
	uuid := "test-uuid-structure"
	timestamp := time.Date(2025, 11, 17, 12, 0, 0, 0, time.UTC)

	// Create record with specific data
	record := createDummyArchivalRecord(timestamp)

	// Append record
	err := client.AppendTCPInfo(ctx, uuid, record)
	if err != nil {
		t.Fatalf("AppendTCPInfo failed: %v", err)
	}

	// Retrieve and verify structure
	history, err := client.GetTCPInfoHistory(ctx, uuid)
	if err != nil {
		t.Fatalf("GetTCPInfoHistory failed: %v", err)
	}

	if len(history) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(history))
	}

	info := history[0]

	// Verify UUID field
	if info.UUID != uuid {
		t.Errorf("Expected UUID %s, got %s", uuid, info.UUID)
	}

	// Verify Data field contains the archival record data
	if info.Data == nil {
		t.Fatal("Data field is nil")
	}

	// Check that RawIDM data is present
	if _, ok := info.Data["RawIDM"]; !ok {
		t.Error("Expected RawIDM field in Data")
	}

	// Check that Attributes data is present
	if _, ok := info.Data["Attributes"]; !ok {
		t.Error("Expected Attributes field in Data")
	}

	// Check that Timestamp is present
	if _, ok := info.Data["Timestamp"]; !ok {
		t.Error("Expected Timestamp field in Data")
	}
}

func TestPing(t *testing.T) {
	client, mr := createTestClient(t)
	defer mr.Close()
	defer client.Close()

	err := client.Ping()
	if err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

func TestClose(t *testing.T) {
	client, mr := createTestClient(t)
	defer mr.Close()

	err := client.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify client is closed by attempting to ping
	err = client.Ping()
	if err == nil {
		t.Error("Expected error after closing client, got nil")
	}
}
