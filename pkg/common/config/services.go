package config

type Services struct {
	Port     int           `yaml:"port" validate:"required,min=1,max=65535"`
	Worker   WorkerConfig  `yaml:"worker"`
	DBPath   string        `yaml:"db_path"` // SQLite file holding all indexer state; default data/indexer.db
	Webhook  WebhookConfig `yaml:"webhook"`
	APIToken string        `yaml:"api_token"` // bearer token for POST /addresses and /manual-ranges; empty disables them
}

type WorkerConfig struct {
	Regular   WorkerModeConfig `yaml:"regular"`
	Rescanner WorkerModeConfig `yaml:"rescanner"`
	Manual    WorkerModeConfig `yaml:"manual"`
	Catchup   WorkerModeConfig `yaml:"catchup"`
	Mempool   WorkerModeConfig `yaml:"mempool"`
}

type WorkerModeConfig struct {
	Enabled bool `yaml:"enabled"`
}

type WebhookConfig struct {
	URL    string `yaml:"url"`    // matched events are POSTed here; empty keeps them in the outbox table
	Secret string `yaml:"secret"` // optional: signs each body as X-Signature-256: sha256=<hex hmac>
}
