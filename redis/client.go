// Package redis provides a client for writing TCP connection data to Redis.
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"time"

	"github.com/m-lab/tcp-info/netlink"
	"github.com/redis/go-redis/v9"
)

const (
	table1Prefix = "table_1:"
)

// TCPInfo represents TCP-Info data structure for Table 1 operations.
type TCPInfo struct {
	UUID string         `json:"uuid"`
	Data map[string]any `json:"data"`
}

// Client wraps the Redis client with methods for storing TCP connection data.
type Client struct {
	rdb          *redis.Client
	ctx          context.Context
	recordBuffer map[string][]*netlink.ArchivalRecord // Buffer for aggregating records per UUID
	lastLogClear time.Time                            // Track when logs were last cleared
}

// NewClient creates a new Redis client using the REDIS_ADDR environment variable.
// If REDIS_ADDR is not set, it defaults to "localhost:6379".
func NewClient(ctx context.Context) (*Client, error) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     "", // No password by default
		DB:           0,  // Use default DB
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	// Test the connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis at %s: %w", addr, err)
	}

	// log.Printf("Connected to Redis at %s", addr)

	return &Client{
		rdb:          rdb,
		ctx:          ctx,
		recordBuffer: make(map[string][]*netlink.ArchivalRecord),
		lastLogClear: time.Now(),
	}, nil
}

// AppendTCPInfo buffers ArchivalRecords and aggregates every 10 records before
// writing the aggregate to Redis. The aggregate is formatted to match the PyTorch
// model's feature extraction requirements.
// 
// NOTE: This function is called sequentially per UUID from saver.go's handleType(),
// so no mutex is needed for the buffer.
func (c *Client) AppendTCPInfo(ctx context.Context, uuid string, record *netlink.ArchivalRecord) error {
	// Write to log file - using /logs which is mounted as a Docker volume
	logDir := "/logs"
	os.MkdirAll(logDir, 0755)

	// Check if 1 second has elapsed since last clear
	if time.Since(c.lastLogClear) >= 1000*time.Millisecond {
		// Clear the log file by truncating it
		if err := os.Truncate("/logs/latency.log", 0); err != nil {
			log.Printf("Warning: Failed to clear latency.log: %v", err)
		}
		c.lastLogClear = time.Now()
	}

	f, err := os.OpenFile("/logs/latency.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Warning: Failed to open latency.log: %v", err)
	} else {
		defer f.Close()
		fileLogger := log.New(f, "", log.LstdFlags)
		fileLogger.Printf("Entered AppendTCPInfo() for UUID=%s", uuid)
	}

	if record == nil {
		return fmt.Errorf("cannot write nil record")
	}

	// Add record to buffer for this UUID
	c.recordBuffer[uuid] = append(c.recordBuffer[uuid], record)
	bufferLen := len(c.recordBuffer[uuid])

	log.Printf("Buffer for UUID %s now has %d records", uuid, bufferLen)

	// Only aggregate and write when we have 10 records
	if bufferLen < 10 {
		return nil
	}

	// We have 10 records, aggregate them
	records := c.recordBuffer[uuid]
	c.recordBuffer[uuid] = nil // Clear the buffer

	// Process records into PyTorch model format
	aggregated := processRecordsForModel(records)

	// Add metadata
	aggregated["UUID"] = uuid
	aggregated["RecordCount"] = len(records)
	aggregated["Timestamp"] = time.Now().Unix()

	// Serialize aggregated data to JSON string
	data, err := json.Marshal(aggregated)
	if err != nil {
		return fmt.Errorf("failed to marshal aggregated data: %w", err)
	}

	// Key format: table_1:<uuid>
	key := table1Prefix + uuid

	// Append to list (RPUSH adds to the right/end of the list)
	start := time.Now()
	if err := c.rdb.RPush(ctx, key, string(data)).Err(); err != nil {
		return fmt.Errorf("failed to append to Redis list: %w", err)
	}
	elapsed := float64(time.Since(start)) / float64(time.Millisecond)
	log.Printf("---append aggregated JSON to Redis DB: %.3f ms", elapsed)

	// Trim list to keep only last 1000 entries
	start = time.Now()
	if err := c.rdb.LTrim(ctx, key, -1000, -1).Err(); err != nil {
		return fmt.Errorf("failed to trim Redis list: %w", err)
	}
	elapsed = float64(time.Since(start)) / float64(time.Millisecond)
	log.Printf("---trim Redis DB: %.3f ms", elapsed)

	start = time.Now()
	// Set expiration on the list key (24 hours)
	if err := c.rdb.Expire(ctx, key, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("failed to set expiration: %w", err)
	}
	elapsed = float64(time.Since(start)) / float64(time.Millisecond)
	log.Printf("---set expiration of list: %.3f ms", elapsed)
	log.Printf("Successfully wrote aggregated data for %d records to Redis", len(records))
	return nil
}

// processRecordsForModel converts records into the format expected by the PyTorch model.
// It computes the 13 features that get_features() expects.
// 
// Note: ArchivalRecord stores raw netlink data. We extract TCP metrics by parsing
// the raw data or using the record's helper methods like GetStats().
func processRecordsForModel(records []*netlink.ArchivalRecord) map[string]any {
	if len(records) == 0 {
		return map[string]any{}
	}

	// Collect metrics using available methods and fields
	var bytesSent []uint64
	var bytesReceived []uint64
	
	for _, record := range records {
		// Use GetStats() method which we know exists
		sent, received := record.GetStats()
		if sent > 0 {
			bytesSent = append(bytesSent, sent)
		}
		if received > 0 {
			bytesReceived = append(bytesReceived, received)
		}
	}

	// Convert to float64 for calculations
	var bytesSentFloat []float64
	var bytesReceivedFloat []float64
	for _, v := range bytesSent {
		bytesSentFloat = append(bytesSentFloat, float64(v))
	}
	for _, v := range bytesReceived {
		bytesReceivedFloat = append(bytesReceivedFloat, float64(v))
	}

	// Estimate packet lengths (assuming average packet size)
	// This is a placeholder - adjust based on actual available data
	avgPacketSize := 1500.0 // Standard MTU
	var packetLengths []float64
	for range records {
		packetLengths = append(packetLengths, avgPacketSize)
	}

	// Calculate bytes in flight (difference between sent and received)
	var bytesInFlight []float64
	minLen := len(bytesSent)
	if len(bytesReceived) < minLen {
		minLen = len(bytesReceived)
	}
	for i := 0; i < minLen; i++ {
		if bytesSent[i] >= bytesReceived[i] {
			bytesInFlight = append(bytesInFlight, float64(bytesSent[i]-bytesReceived[i]))
		}
	}

	// Placeholder values for metrics we can't extract yet
	// You'll need to parse the raw netlink data to get these
	var ackRtts []float64
	var tcpWindowSizes []float64
	var totalRetransmissions uint64
	var totalDuplicateAcks uint64
	var bbrPipeFull int64

	// Compute aggregates matching the Python feature set
	result := map[string]any{
		// Packet length features (using estimates)
		"sum_packet_length":    sum64(packetLengths),
		"avg_packet_length":    avg(packetLengths),
		"stddev_packet_length": stddev(packetLengths),

		// Bytes in flight features
		"sum_bytes_in_flight":    sum64(bytesInFlight),
		"avg_bytes_in_flight":    avg(bytesInFlight),
		"stddev_bytes_in_flight": stddev(bytesInFlight),

		// ACK RTT features (placeholder - need to parse raw data)
		"sum_ack_rtt":    sum64(ackRtts),
		"avg_ack_rtt":    avg(ackRtts),
		"stddev_ack_rtt": stddev(ackRtts),

		// Other features
		"avg_tcp_window_size": avg(tcpWindowSizes),
		"sum_retransmission":  totalRetransmissions,
		"sum_duplicate_ack":   totalDuplicateAcks,
		"bbrpipefull":         bbrPipeFull,
		
		// Additional info we can extract
		"total_bytes_sent":     sumUint64(bytesSent),
		"total_bytes_received": sumUint64(bytesReceived),
		"avg_bytes_sent":       avg(bytesSentFloat),
		"avg_bytes_received":   avg(bytesReceivedFloat),
	}

	return result
}

// Helper functions for statistics

func avg(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func sum64(values []float64) float64 {
	var total float64
	for _, v := range values {
		total += v
	}
	return total
}

func sumUint64(values []uint64) uint64 {
	var total uint64
	for _, v := range values {
		total += v
	}
	return total
}

func stddev(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	// Calculate mean
	mean := avg(values)

	// Calculate variance
	var variance float64
	for _, v := range values {
		diff := v - mean
		variance += diff * diff
	}
	variance /= float64(len(values))

	// Return standard deviation (square root of variance)
	return math.Sqrt(variance)
}

// GetTCPInfoHistory retrieves all historical TCPInfo snapshots for a connection.
func (c *Client) GetTCPInfoHistory(ctx context.Context, uuid string) ([]*TCPInfo, error) {
	key := table1Prefix + uuid

	// Get all items from the list
	results, err := c.rdb.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get history: %w", err)
	}

	infos := make([]*TCPInfo, 0, len(results))
	for _, data := range results {
		var info TCPInfo
		if err := json.Unmarshal([]byte(data), &info); err != nil {
			log.Printf("Warning: failed to unmarshal TCPInfo: %v", err)
			continue
		}
		infos = append(infos, &info)
	}

	return infos, nil
}

// Close closes the Redis connection.
func (c *Client) Close() error {
	if c.rdb != nil {
		return c.rdb.Close()
	}
	return nil
}

// Ping checks if the Redis connection is alive.
func (c *Client) Ping() error {
	return c.rdb.Ping(c.ctx).Err()
}