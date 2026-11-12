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
//	  tasks: [import, selftest]    # facoltativo: i task attivabili (vuoto = tutti quelli dichiarati)
type Config struct {
	// Disabled spegne l'autenticazione: ogni richiesta è un chiamante anonimo. Solo sviluppo. È un
	// "disabled" e non un "enabled" perché lo zero value deve essere quello sicuro.
	Disabled bool `yaml:"disabled" mapstructure:"disabled" json:"disabled"`
	// Token è il segreto condiviso con chi chiama (il Job/CronJob con esegui.sh): lo stesso Secret
	// del namespace, montato nell'applicativo come variabile d'ambiente e nel job come file.
	// Obbligatorio se l'autenticazione è attiva.
	Token string `yaml:"token" mapstructure:"token" json:"-"`
	// Tasks limita i task attivabili; vuoto = tutti quelli della sezione `tasks:` del batch.
	Tasks []string `yaml:"tasks" mapstructure:"tasks" json:"tasks"`
	// MaxWait è il tetto del long-poll (default DefaultMaxWait).
	MaxWait time.Duration `yaml:"max-wait" mapstructure:"max-wait" json:"max-wait"`
	// RunTTL è la durata massima di una run S3 (default DefaultRunTTL).
	RunTTL time.Duration `yaml:"run-ttl" mapstructure:"run-ttl" json:"run-ttl"`
}

// Validate controlla la Config. È chiamata da Module: un trigger senza token non deve partire,
// perché esporrebbe l'avvio dei task a chiunque raggiunga l'applicativo.
func (c *Config) Validate() error {
	if !c.Disabled && c.Token == "" {
		return errors.New("trigger: token obbligatorio (services.trigger.token, dal Secret condiviso); disabled: true solo in sviluppo")
	}
	// La sostituzione delle variabili d'ambiente della config (tpm-common) lascia `${NOME}` così
	// com'è quando la variabile non è definita: il token sarebbe il letterale scritto nello YAML,
	// cioè noto a chiunque legga il repository. Un Secret non montato deve fermare l'avvio, non
	// aprire l'API con un token pubblico.
	if !c.Disabled && strings.Contains(c.Token, "${") {
		return errors.New("trigger: il token è il segnaposto " + c.Token + " non risolto: la variabile d'ambiente non è definita (Secret non montato?)")
	}
	if c.MaxWait < 0 || c.RunTTL < 0 {
		return errors.New("trigger: max-wait e run-ttl non possono essere negativi")
	}
	return nil
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
