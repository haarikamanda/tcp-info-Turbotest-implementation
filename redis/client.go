// Package redis provides a client for writing TCP connection data to Redis.
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	rdb *redis.Client
	ctx context.Context
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

	log.Printf("Connected to Redis at %s", addr)

	return &Client{
		rdb: rdb,
		ctx: ctx,
	}, nil
}

// AppendTCPInfo converts an ArchivalRecord to TCPInfo format and appends it to a
// time-series list in Redis. The list key format is "table_1:<uuid>".
// This maintains a historical time series of TCPInfo objects for each speed test.
// The list has a 24-hour TTL and keeps the last 1000 entries.
func (c *Client) AppendTCPInfo(ctx context.Context, uuid string, record *netlink.ArchivalRecord) error {
	// Write to file
	newDir := "../logs"
	err := os.Chdir(newDir)
	if err != nil {
		os.Mkdir(newDir, os.FileMode(os.O_CREATE))
		os.Chdir(newDir)
	}
	f, err := os.OpenFile("latency.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	log.SetOutput(f)

	// Unix() returns the number of seconds elapsed since January 1, 1970 UTC
	log.Printf("Entered AppendTCPInfo()")
	if record == nil {
		return fmt.Errorf("cannot write nil record")
	}

	// Convert ArchivalRecord to a map for TCPInfo Data field
	recordBytes, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	var dataMap map[string]any
	if err := json.Unmarshal(recordBytes, &dataMap); err != nil {
		return fmt.Errorf("failed to unmarshal to map: %w", err)
	}

	// Create TCPInfo object
	info := &TCPInfo{
		UUID: uuid,
		Data: dataMap,
	}

	// Serialize TCPInfo to JSON
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal TCPInfo: %w", err)
	}

	// Key format: table_1:<uuid>
	key := table1Prefix + uuid

	// Append to list (RPUSH adds to the right/end of the list)
	start := time.Now()
	if err := c.rdb.RPush(ctx, key, data).Err(); err != nil {
		return fmt.Errorf("failed to append to Redis list: %w", err)
	}
	elapsed := float64(time.Since(start)) / float64(time.Millisecond)
	log.Printf("---append JSON to Redis DB: %.3f ms", elapsed)

	// Trim list to keep only last 1000 entries to prevent unbounded growth
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

	return nil
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
