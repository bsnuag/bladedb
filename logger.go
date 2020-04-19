package bladedb

import (
	"fmt"
	"github.com/rs/zerolog"
	"os"
	"strings"
	"time"
)

func setupLogger() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}

	output.FormatLevel = func(i interface{}) string {
		return strings.ToUpper(fmt.Sprintf("| %-6s|", i))
	}
	output.FormatMessage = func(i interface{}) string {
		return fmt.Sprintf("%s", i)
	}
	output.FormatFieldName = func(i interface{}) string {
		return fmt.Sprintf("%s:", i)
	}
	output.FormatFieldValue = func(i interface{}) string {
		return fmt.Sprintf("%s", i)
	}
	db.logger = zerolog.New(output).With().Timestamp().Logger()
}
