package main

import (
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"

	api "github.com/eclipse-basyx/basyx-go-sdk/internal/submodelrepository/api"
	persistence_postgresql "github.com/eclipse-basyx/basyx-go-sdk/internal/submodelrepository/persistence/postgresql"
	openapi "github.com/eclipse-basyx/basyx-go-sdk/pkg/submodelrepositoryapi/go"
)

func main() {
	// Create Chi router
	r := chi.NewRouter()

	// Enable CORS
	c := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{http.MethodGet, http.MethodPost, http.MethodDelete, http.MethodOptions},
		AllowedHeaders:   []string{"*"},
		AllowCredentials: true,
	})
	r.Use(c.Handler)

	// Add health endpoint
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Instantiate generated services & controllers
	// ==== Discovery Service ====
	smDatabase, err := persistence_postgresql.NewPostgreSQLSubmodelBackend("postgres://postgres:postgres@localhost:5433/basyx?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
		return
	}
	smSvc := api.NewSubmodelRepositoryAPIAPIService(smDatabase)
	smCtrl := openapi.NewSubmodelRepositoryAPIAPIController(smSvc)
	for _, rt := range smCtrl.Routes() {
		r.Method(rt.Method, rt.Pattern, rt.HandlerFunc)
	}

	// ==== Description Service ====
	descSvc := openapi.NewDescriptionAPIAPIService()
	descCtrl := openapi.NewDescriptionAPIAPIController(descSvc)
	for _, rt := range descCtrl.Routes() {
		r.Method(rt.Method, rt.Pattern, rt.HandlerFunc)
	}

	// Start the server
	addr := "0.0.0.0:5004"
	log.Printf("▶️  Submodel Repository listening on %s\n", addr)
	log.Fatal(http.ListenAndServe(addr, r))
}
