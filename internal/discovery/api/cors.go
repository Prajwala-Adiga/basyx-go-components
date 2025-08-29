package api

import (
	"github.com/eclipse-basyx/basyx-go-sdk/internal/discovery/config"
	"github.com/go-chi/cors"
)

func ConfigureCors(cfg *config.Config) *cors.Cors {
	c := cors.New(cors.Options{
		AllowedOrigins:   cfg.CORS.AllowedOrigins,
		AllowedMethods:   cfg.CORS.AllowedMethods,
		AllowedHeaders:   cfg.CORS.AllowedHeaders,
		AllowCredentials: cfg.CORS.AllowCredentials,
	})
	return c
}
