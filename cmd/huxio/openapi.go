package main

import (
	"os"

	inboundhttp "github.com/plusiv/huxio/internal/adapters/inbound/http"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/handlers"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/openapi"
	"github.com/plusiv/huxio/internal/infrastructure/telemetry"
	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
)

func newOpenAPICmd() *cobra.Command {
	var (
		output string
		server string
	)

	cmd := &cobra.Command{
		Use:   "openapi",
		Short: "Print the OpenAPI document for the HTTP API",
		Long: "The document is generated from the routes the server actually registers and\n" +
			"the payload types the binder validates, so it cannot drift from the API.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// A router with no dependencies is enough: only the route table and the
			// payload types are read, never a handler.
			router := inboundhttp.NewRouter(inboundhttp.RouterDeps{
				Metrics:            telemetry.New(),
				HealthHandler:      &handlers.HealthHandler{},
				ApplicationHandler: &handlers.ApplicationHandler{},
				EndpointHandler:    &handlers.EndpointHandler{},
				EventTypeHandler:   &handlers.EventTypeHandler{},
				MessageHandler:     &handlers.MessageHandler{},
				AttemptHandler:     &handlers.AttemptHandler{},
				PortalTokenHandler: &handlers.PortalHandler{},
				AdminHandler:       &handlers.AdminHandler{},
				StreamHandler:      &handlers.StreamHandler{},
			})

			document := inboundhttp.Spec(router, openapi.Info{
				Title:   "huxio",
				Version: version,
				Description: "Self-hosted webhook delivery. The surface mirrors the incumbent's REST API, " +
					"so its client SDKs work unmodified; additions live on their own paths.",
			}, []openapi.Server{{URL: server, Description: "This deployment"}})

			encoded, err := document.JSON()
			if err != nil {
				return err
			}
			encoded = append(encoded, '\n')

			if output == "" || output == "-" {
				_, err := cmd.OutOrStdout().Write(encoded)
				return err
			}
			if err := os.WriteFile(output, encoded, 0o644); err != nil {
				return eris.Wrap(err, "write openapi document")
			}
			cmd.PrintErrf("wrote %s\n", output)
			return nil
		},
	}

	cmd.Flags().StringVarP(&output, "out", "o", "-", "write to a file instead of stdout")
	cmd.Flags().StringVar(&server, "server", "http://localhost:8080", "server URL to advertise in the document")
	return cmd
}
