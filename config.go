package trigger

import (
	"errors"
	"strings"
	"time"
)

// Default della Config.
const (
	// DefaultMaxWait è il tetto del long-poll di GET /runs/{runId}: deve restare sotto il timeout
	// della Route/haproxy davanti all'applicativo.
	DefaultMaxWait = 50 * time.Second
	// DefaultRunTTL è la durata massima di una run S3: oltre, al primo sguardo si chiude ERROR (S3
	// irraggiungibile da ore, esecutore fermo).
	DefaultRunTTL = 12 * time.Hour
)

// Config è la sezione `services.trigger` dell'applicativo.
//
//	trigger:
//	  token: "${TRIGGER_TOKEN}"   # dal Secret condiviso del namespace
//	  tasks: [import, selftest]   # facoltativo: i task attivabili (vuoto = tutti quelli dichiarati)
type Config struct {
	// Token è il segreto condiviso con chi chiama (il Job/CronJob con esegui.sh): lo stesso Secret
	// del namespace, montato nell'applicativo come variabile d'ambiente e nel job come file.
	// Obbligatorio, salvo con `develop-mode: true` di go-core-api: lì, se manca, l'autenticazione è
	// spenta (vedi Register).
	Token string `yaml:"token" mapstructure:"token" json:"-"`
	// Tasks limita i task attivabili; vuoto = tutti quelli della sezione `tasks:` del batch.
	Tasks []string `yaml:"tasks" mapstructure:"tasks" json:"tasks"`
	// MaxWait è il tetto del long-poll (default DefaultMaxWait).
	MaxWait time.Duration `yaml:"max-wait" mapstructure:"max-wait" json:"max-wait"`
	// RunTTL è la durata massima di una run S3 (default DefaultRunTTL).
	RunTTL time.Duration `yaml:"run-ttl" mapstructure:"run-ttl" json:"run-ttl"`
}

// Validate controlla la Config; è chiamata da Module. Il token non si controlla qui: se sia
// obbligatorio dipende dal develop-mode di go-core-api, che si conosce solo in Register.
func (c *Config) Validate() error {
	if c.MaxWait < 0 || c.RunTTL < 0 {
		return errors.New("trigger: max-wait e run-ttl non possono essere negativi")
	}
	return nil
}

// tokenProblem dice perché il token non è utilizzabile, "" se lo è.
//
// La sostituzione delle variabili d'ambiente della config (tpm-common) lascia `${NOME}` così com'è
// quando la variabile non è definita: il token sarebbe il letterale scritto nello YAML, cioè noto a
// chiunque legga il repository. Un segnaposto non risolto vale quindi come token assente.
func (c *Config) tokenProblem() string {
	switch {
	case c.Token == "":
		return "token assente (services.trigger.token, dal Secret condiviso)"
	case strings.Contains(c.Token, "${"):
		return "il token è il segnaposto " + c.Token + " non risolto: la variabile d'ambiente non è definita (Secret non montato?)"
	}
	return ""
}

func (c *Config) maxWait() time.Duration {
	if c.MaxWait <= 0 {
		return DefaultMaxWait
	}
	return c.MaxWait
}

func (c *Config) runTTL() time.Duration {
	if c.RunTTL <= 0 {
		return DefaultRunTTL
	}
	return c.RunTTL
}
