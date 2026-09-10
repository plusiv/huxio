package main

import (
	"github.com/plusiv/huxio/internal/infrastructure/secrets"
	"github.com/spf13/cobra"
)

func newKeygenCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "keygen",
		Short: "Generate an encryption key for HUXIO_ENCRYPTION_KEY",
		Long: "Generate a base64 XChaCha20-Poly1305 key used to seal endpoint signing\n" +
			"secrets at rest. Set it as HUXIO_ENCRYPTION_KEY; to rotate, put the new key\n" +
			"first and keep the previous one in the comma-separated list.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			key, err := secrets.GenerateKey()
			if err != nil {
				return err
			}
			cmd.Println(key)
			return nil
		},
	}
}
