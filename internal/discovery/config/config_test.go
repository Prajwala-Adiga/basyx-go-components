//go:build unit
// +build unit

package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadConfigDefaultValues(t *testing.T) {
	// Test loading with no config file (should use defaults)
	cfg, err := LoadConfig("")
	assert.NoError(t, err)
	assert.NotNil(t, cfg)

	// Check that defaults were set correctly
	assert.Equal(t, "0.0.0.0", cfg.Server.Host)
	assert.Equal(t, "5000", cfg.Server.Port)
	assert.Equal(t, "", cfg.Server.ContextPath)

	assert.Equal(t, "mongodb://localhost:27017", cfg.MongoDB.URI)
	assert.Equal(t, "basyx-go", cfg.MongoDB.Database)
	assert.Equal(t, "aas_discovery", cfg.MongoDB.Collection)
	assert.Equal(t, 10, cfg.MongoDB.Timeout)

	assert.Equal(t, []string{"*"}, cfg.CORS.AllowedOrigins)
	assert.Equal(t, []string{"GET", "POST", "DELETE", "OPTIONS"}, cfg.CORS.AllowedMethods)
	assert.Equal(t, []string{"*"}, cfg.CORS.AllowedHeaders)
	assert.Equal(t, true, cfg.CORS.AllowCredentials)

	assert.Equal(t, "InMemory", cfg.BaSyx.Backend)
}

func TestLoadConfigFromFile(t *testing.T) {
	// Create a temporary config file
	configContent := `
{
	"server": {
		"host": "127.0.0.1",
		"port": "8080",
		"contextPath": "/api"
	},
	"mongodb": {
		"uri": "mongodb://testuser:testpass@localhost:27018",
		"database": "test-db",
		"collection": "test-collection",
		"timeout": 15
	},
	"cors": {
		"allowedOrigins": ["http://localhost:3000"],
		"allowedMethods": ["GET", "POST"],
		"allowedHeaders": ["Content-Type", "Authorization"],
		"allowCredentials": false
	},
	"basyx": {
		"backend": "MongoDB"
	}
}
`

	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")
	err := os.WriteFile(configPath, []byte(configContent), 0644)
	assert.NoError(t, err)

	// Load config from file
	cfg, err := LoadConfig(configPath)
	assert.NoError(t, err)
	assert.NotNil(t, cfg)

	// Check that values from file were used
	assert.Equal(t, "127.0.0.1", cfg.Server.Host)
	assert.Equal(t, "8080", cfg.Server.Port)
	assert.Equal(t, "/api", cfg.Server.ContextPath)

	assert.Equal(t, "mongodb://testuser:testpass@localhost:27018", cfg.MongoDB.URI)
	assert.Equal(t, "test-db", cfg.MongoDB.Database)
	assert.Equal(t, "test-collection", cfg.MongoDB.Collection)
	assert.Equal(t, 15, cfg.MongoDB.Timeout)

	assert.Equal(t, []string{"http://localhost:3000"}, cfg.CORS.AllowedOrigins)
	assert.Equal(t, []string{"GET", "POST"}, cfg.CORS.AllowedMethods)
	assert.Equal(t, []string{"Content-Type", "Authorization"}, cfg.CORS.AllowedHeaders)
	assert.Equal(t, false, cfg.CORS.AllowCredentials)

	assert.Equal(t, "MongoDB", cfg.BaSyx.Backend)
}

func TestLoadConfigFromInvalidFile(t *testing.T) {
	// Create an invalid config file
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "invalid.json")
	err := os.WriteFile(configPath, []byte("invalid json"), 0644)
	assert.NoError(t, err)

	// Attempt to load invalid config
	cfg, err := LoadConfig(configPath)
	assert.Error(t, err)
	assert.Nil(t, cfg)
}

func TestLoadConfigFromNonExistentFile(t *testing.T) {
	// Try to load from a file that doesn't exist
	cfg, err := LoadConfig("/path/to/nonexistent/config.json")
	assert.Error(t, err)
	assert.Nil(t, cfg)
}

func TestLoadConfigFromEnvVars(t *testing.T) {
	// Set environment variables
	os.Setenv("SERVER_HOST", "localhost")
	os.Setenv("SERVER_PORT", "9000")
	os.Setenv("MONGODB_URI", "mongodb://127.0.0.1:27017/test")
	defer func() {
		os.Unsetenv("SERVER_HOST")
		os.Unsetenv("SERVER_PORT")
		os.Unsetenv("MONGODB_URI")
	}()

	// Load config without a file (should use env vars and defaults)
	cfg, err := LoadConfig("")
	assert.NoError(t, err)
	assert.NotNil(t, cfg)

	// Check that environment variables were used
	assert.Equal(t, "localhost", cfg.Server.Host)
	assert.Equal(t, "9000", cfg.Server.Port)
	assert.Equal(t, "mongodb://127.0.0.1:27017/test", cfg.MongoDB.URI)
}

func TestRedactMongoURI(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Standard URI with credentials",
			input:    "mongodb://user:password@localhost:27017",
			expected: "mongodb://****:****@localhost:27017",
		},
		{
			name:     "URI with srv and credentials",
			input:    "mongodb+srv://user:password@cluster.mongodb.net",
			expected: "mongodb+srv://****:****@cluster.mongodb.net",
		},
		{
			name:     "URI without credentials",
			input:    "mongodb://localhost:27017",
			expected: "mongodb://localhost:27017",
		},
		{
			name:     "Non-MongoDB URI",
			input:    "http://example.com",
			expected: "http://example.com",
		},
		{
			name:     "Empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := redactMongoURI(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestPrintConfiguration(t *testing.T) {
	// Create a config with sensitive information
	cfg := &Config{
		Server: ServerConfig{
			Host:        "localhost",
			Port:        "8080",
			ContextPath: "/api",
		},
		MongoDB: MongoDBConfig{
			URI:        "mongodb://user:password@localhost:27017",
			Database:   "test",
			Collection: "users",
			Timeout:    10,
		},
		CORS: CORSConfig{
			AllowedOrigins:   []string{"*"},
			AllowedMethods:   []string{"GET", "POST"},
			AllowedHeaders:   []string{"Content-Type"},
			AllowCredentials: true,
		},
		BaSyx: BaSyxConfig{
			Backend: "MongoDB",
		},
	}

	// Capture logger output (note: this is a simplification, in a real test
	// you might want to redirect log output to a buffer and inspect it)
	// Here we're just testing that the function doesn't panic
	PrintConfiguration(cfg)

	// Also verify that the original config is not modified
	assert.Equal(t, "mongodb://user:password@localhost:27017", cfg.MongoDB.URI)

	// Check serialization capability for the config
	_, err := json.Marshal(cfg)
	assert.NoError(t, err)
}
