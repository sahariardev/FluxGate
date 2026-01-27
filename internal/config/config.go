package config

type Config struct {
	Logging LoggingConfig `yaml:"logging"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}
