package config

import (
	"encoding/json"
	"flag"
	"log"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/spf13/viper"
)

// Config holds all configuration for the application
type Config struct {
	Server  ServerConfig  `mapstructure:"server"`
	MongoDB MongoDBConfig `mapstructure:"mongodb"`
	S3      S3Config      `mapstructure:"s3"`
	CORS    CORSConfig    `mapstructure:"cors"`
	BaSyx   BaSyxConfig   `mapstructure:"basyx"`
}

// ServerConfig holds server-specific configuration
type ServerConfig struct {
	Host        string `mapstructure:"host"`
	Port        string `mapstructure:"port"`
	ContextPath string `mapstructure:"contextPath"` // Used for base URL path
}

// MongoDBConfig holds MongoDB connection configuration
type MongoDBConfig struct {
	URI        string `mapstructure:"uri"`
	Database   string `mapstructure:"database"`
	Collection string `mapstructure:"collection"`
	Timeout    int    `mapstructure:"timeout"` // In seconds
}

type S3Config struct {
	Region       string `mapstructure:"region"`
	Endpoint     string `mapstructure:"endpoint"`
	Bucket       string `mapstructure:"bucket"`
	Prefix       string `mapstructure:"prefix"`
	AccessKey    string `mapstructure:"accessKey"`
	SecretKey    string `mapstructure:"secretKey"`
	CacheMinutes int    `mapstructure:"cacheMinutes"`
}

// CORSConfig holds CORS configuration
type CORSConfig struct {
	AllowedOrigins   []string `mapstructure:"allowedOrigins"`
	AllowedMethods   []string `mapstructure:"allowedMethods"`
	AllowedHeaders   []string `mapstructure:"allowedHeaders"`
	AllowCredentials bool     `mapstructure:"allowCredentials"`
}

// BaSyxConfig holds Basyx-specific configuration
type BaSyxConfig struct {
	Backend string `mapstructure:"backend"` // "InMemory" or "MongoDB" or "S3"
}

// LoadConfig loads the configuration from files and environment variables
func LoadConfig(configPath string) (*Config, error) {
	v := viper.New()

	// Set default values
	setDefaults(v)

	// Read config file if provided
	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			return nil, err
		}
	}

	// Override config with environment variables
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

// setDefaults sets sensible defaults for configuration
func setDefaults(v *viper.Viper) {
	// Server defaults
	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", "5000")
	v.SetDefault("server.contextPath", "")

	// MongoDB defaults
	v.SetDefault("mongodb.uri", "mongodb://localhost:27017")
	v.SetDefault("mongodb.database", "basyx-go")
	v.SetDefault("mongodb.collection", "aas_discovery")
	v.SetDefault("mongodb.timeout", 10)

	// S3 defaults
	v.SetDefault("s3.region", "eu-central-1")
	v.SetDefault("s3.endpoint", "http://localhost:9000")
	v.SetDefault("s3.bucket", "basyx")
	v.SetDefault("s3.prefix", "aas_discovery")
	v.SetDefault("s3.accessKey", "minioadmin")
	v.SetDefault("s3.secretKey", "minioadmin")
	v.SetDefault("s3.cacheMinutes", 5)

	// CORS defaults
	v.SetDefault("cors.allowedOrigins", []string{"*"})
	v.SetDefault("cors.allowedMethods", []string{"GET", "POST", "DELETE", "OPTIONS"})
	v.SetDefault("cors.allowedHeaders", []string{"*"})
	v.SetDefault("cors.allowCredentials", true)

	// BaSyx defaults
	v.SetDefault("basyx.backend", "InMemory") // Default to MongoDB
}

// PrintConfiguration prints the current configuration with sensitive data redacted
func PrintConfiguration(cfg *Config) {
	// Create a copy of the config to avoid modifying the original
	cfgCopy := *cfg

	// Redact sensitive information if present in the MongoDB URI
	if cfg.MongoDB.URI != "" {
		// Simple redaction that preserves the structure but hides credentials
		cfgCopy.MongoDB.URI = redactMongoURI(cfg.MongoDB.URI)
	}

	// Redact S3 credentials
	redactS3Credentials(&cfgCopy)

	// Convert to JSON for pretty printing
	configJSON, err := json.MarshalIndent(cfgCopy, "", "  ")
	if err != nil {
		log.Printf("Unable to marshal configuration to JSON: %v", err)
		return
	}

	log.Printf("Configuration:\n%s", string(configJSON))
}

func redactMongoURI(uri string) string {
	// Look for mongodb:// or mongodb+srv:// prefix
	var prefix string
	if strings.HasPrefix(uri, "mongodb://") {
		prefix = "mongodb://"
	} else if strings.HasPrefix(uri, "mongodb+srv://") {
		prefix = "mongodb+srv://"
	} else {
		return uri // Not a standard MongoDB URI
	}

	// Skip the prefix
	uriWithoutPrefix := uri[len(prefix):]

	// Check for @ symbol which indicates credentials are present
	atIndex := strings.Index(uriWithoutPrefix, "@")
	if atIndex == -1 {
		return uri // No credentials in the URI
	}

	// Replace everything between prefix and @ with "****:****"
	return prefix + "****:****" + uriWithoutPrefix[atIndex:]
}

func redactS3Credentials(cfg *Config) {
	// Redact sensitive information in S3 configuration
	if cfg.S3.AccessKey != "" {
		cfg.S3.AccessKey = "****"
	}
	if cfg.S3.SecretKey != "" {
		cfg.S3.SecretKey = "****"
	}
}

func ConfigureServer(configPath string) (*Config, *chi.Mux) {
	PrintSplash()

	if configPath == "" {
		cfgPathFlag := flag.String("config", "", "Path to config file")
		flag.Parse()
		configPath = *cfgPathFlag
	}

	// Load configuration
	cfg, err := LoadConfig(configPath)
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
		return nil, nil
	}

	PrintConfiguration(cfg)

	// Create Chi router
	r := chi.NewRouter()
	return cfg, r
}
