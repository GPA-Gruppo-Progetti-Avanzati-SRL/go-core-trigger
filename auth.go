package trigger

import (
	"crypto/sha256"
	"crypto/subtle"
	"strings"

	core "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app"
)

// authenticate verifica il token Bearer contro quello condiviso. Si confrontano gli SHA-256 dei due
// valori e non i valori: subtle.ConstantTimeCompare è a tempo costante solo a parità di lunghezza, e
// confrontando i token in chiaro la durata direbbe quanto è lungo quello giusto.
func authenticate(cfg *Config, authorization string) *core.Error {
	if cfg.Disabled {
		return nil
	}
	token, ok := strings.CutPrefix(strings.TrimSpace(authorization), "Bearer ")
	token = strings.TrimSpace(token)
	if !ok || token == "" {
		return unauthorized(CodeTokenMissing, "token mancante: atteso Authorization: Bearer <token>")
	}
	got := sha256.Sum256([]byte(token))
	want := sha256.Sum256([]byte(cfg.Token))
	if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
		return unauthorized(CodeTokenInvalid, "token non riconosciuto")
	}
	return nil
}
