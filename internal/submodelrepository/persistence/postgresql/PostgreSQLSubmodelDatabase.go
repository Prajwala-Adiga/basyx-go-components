package persistence_postgresql

import (
	"database/sql"
	"encoding/json"
	"errors"

	_ "github.com/lib/pq" // PostgreSQL Treiber

	gen "github.com/eclipse-basyx/basyx-go-sdk/pkg/submodelrepositoryapi/go"
)

type PostgreSQLSubmodelDatabase struct {
	db *sql.DB
}

// Konstruktor
func NewPostgreSQLSubmodelBackend(dsn string) (*PostgreSQLSubmodelDatabase, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &PostgreSQLSubmodelDatabase{db: db}, nil
}

// GetAllSubmodels holt alle Submodelle aus der DB
// func (p *PostgreSQLSubmodelDatabase) GetAllSubmodels() ([]model.Submodel, error) {
// 	rows, err := p.db.Query("SELECT * FROM submodels")
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rows.Close()

// 	var submodels []model.Submodel
// 	for rows.Next() {
// 		var s model.Submodel
// 		if err := rows.Scan(&s); err != nil {
// 			return nil, err
// 		}
// 		submodels = append(submodels, s)
// 	}
// 	return submodels, nil
// }

// GetAllSubmodels holt alle Submodelle aus der DB
func (p *PostgreSQLSubmodelDatabase) GetAllSubmodels() ([]gen.Submodel, error) {
	rows, err := p.db.Query(`SELECT payload FROM submodels ORDER BY id LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := []gen.Submodel{}
	for rows.Next() {
		var js []byte
		if err := rows.Scan(&js); err != nil {
			return nil, err
		}
		var m gen.Submodel
		if err := json.Unmarshal(js, &m); err != nil {
			return nil, err
		}
		list = append(list, m)
	}
	return list, nil
}

// GetSubmodel returns one Submodel by id
func (p *PostgreSQLSubmodelDatabase) GetSubmodel(id string) (gen.Submodel, error) {
	var js []byte
	err := p.db.QueryRow(`SELECT payload FROM submodels WHERE id=$1`, id).Scan(&js)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return gen.Submodel{}, sql.ErrNoRows
		}
		return gen.Submodel{}, err
	}
	var m gen.Submodel
	if err := json.Unmarshal(js, &m); err != nil {
		return gen.Submodel{}, err
	}
	return m, nil
}

// CreateSubmodel erstellt ein neues Submodel
// func (p *PostgreSQLSubmodelDatabase) CreateSubmodel(submodel model.Submodel) (string, error) {
// 	id := submodel.Id
// 	if id == "" {
// 		return "", fmt.Errorf("Id must not be empty")
// 	}
// 	_, err := p.db.Exec("INSERT INTO submodels (id) VALUES ($1, $2)", submodel.Id)
// 	if err != nil {
// 		return "", err
// 	}
// 	return id, nil
// }

// // DeleteSubmodel löscht ein Submodel
// func (p *PostgreSQLSubmodelDatabase) DeleteSubmodel(id string) error {
// 	result, err := p.db.Exec("DELETE FROM submodels WHERE id = $1", id)
// 	if err != nil {
// 		return err
// 	}
// 	rowsAffected, err := result.RowsAffected()
// 	if err != nil {
// 		return err
// 	}
// 	if rowsAffected == 0 {
// 		return fmt.Errorf("no submodel with id %s found", id)
// 	}
// 	return nil
// }
