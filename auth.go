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
//
// open = autenticazione spenta (develop-mode senza token). Senza un token utilizzabile e fuori dal
// develop-mode ogni richiesta è rifiutata: Register lo impedisce già all'avvio, ma un Trigger usato
// senza Register non deve diventare aperto per difetto.
func authenticate(cfg *Config, open bool, authorization string) *core.Error {
	if open {
		return nil
	}
	if problem := cfg.tokenProblem(); problem != "" {
		return unauthorized(CodeTokenInvalid, "trigger non configurato: "+problem)
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
