package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/bxcodec/faker/v3"
)

// Email representa la estructura de un correo electrónico.
type Email struct {
	ID       string `json:"id"`
	Sender   string `json:"sender"`
	Receiver string `json:"receiver"`
	Subject  string `json:"subject"`
	Body     string `json:"body"`
	Date     string `json:"date"` // Formato ISO8601: "2025-01-07T10:00:00Z"
}

const (
	ZincURL      = "http://localhost:4080/api/_bulkv2" // Endpoint de ZincSearch
	ZincUser     = "admin"                             // Usuario de ZincSearch
	ZincPassword = "securepassword"                    // Contraseña de ZincSearch
	TotalEmails  = 5000000                             // Cantidad de correos a generar
	BatchSize    = 1000000                             // Cantidad de correos por lote
)

func main() {
	// Generar y enviar los correos
	fmt.Println("Iniciando la carga de correos electrónicos...")

	start := time.Now()

	for i := 0; i < TotalEmails; i += BatchSize {
		fmt.Printf("Lote %d-%d\n", i+1, i+BatchSize)
		batchStart := time.Now()
		batch := generateEmails(BatchSize, i)
		fmt.Printf("Generación del lote %d-%d completada en %v\n", i+1, i+BatchSize, time.Since(batchStart))

		sendStart := time.Now()
		err := sendBatch(batch)
		if err != nil {
			log.Printf("Error al enviar lote %d-%d: %v\n", i+1, i+BatchSize, err)
		} else {
			fmt.Printf("Envío del lote %d-%d completado en %v\n", i+1, i+BatchSize, time.Since(sendStart))
		}
	}

	fmt.Printf("Proceso completado en %v\n", time.Since(start))
}

// generateEmails genera un lote de correos electrónicos con datos aleatorios.
func generateEmails(count, startID int) []Email {
	emails := make([]Email, count)
	for i := 0; i < count; i++ {
		emails[i] = Email{
			ID:       fmt.Sprintf("%d", startID+i+1),
			Sender:   faker.Email(),
			Receiver: faker.Email(),
			Subject:  faker.Sentence(),
			Body:     faker.Paragraph(),
			Date:     time.Now().Add(time.Duration(-rand.Intn(365*5)) * 24 * time.Hour).Format(time.RFC3339), // Fecha aleatoria en los últimos 5 años
		}
	}
	return emails
}

// sendBatch envía un lote de correos electrónicos a ZincSearch usando el endpoint bulk v2.
func sendBatch(emails []Email) error {
	bulkData := map[string]interface{}{
		"index":   "emails", // Asegúrate de que este índice exista en tu ZincSearch
		"records": emails,
	}

	data, err := json.Marshal(bulkData)
	if err != nil {
		return fmt.Errorf("error al serializar los datos: %v", err)
	}

	req, err := http.NewRequest("POST", ZincURL, bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("error al crear la solicitud: %v", err)
	}

	req.SetBasicAuth(ZincUser, ZincPassword)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error al enviar la solicitud: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error del servidor, código HTTP: %d", resp.StatusCode)
	}

	return nil
}
