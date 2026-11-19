package graph

import (
	"slices"
	"testing"

	"github.com/DIMO-Network/dauth/pkg/tokenclaims"
	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/dq/internal/repositories"
)

func TestHasPrivilegesForSignal(t *testing.T) {
	repo, err := repositories.NewRepository(nil)
	if err != nil {
		t.Fatalf("failed to build repository: %v", err)
	}

	nonLoc := tokenclaims.AbilityTelemetryRead
	allTimeLoc := tokenclaims.AbilityLocationPrecise
	approxLoc := tokenclaims.AbilityLocationApproximate

	tests := []struct {
		name        string
		signal      string
		permissions []string
		want        bool
	}{
		{
			name:        "coordinates allowed with all-time location",
			signal:      "currentLocationCoordinates",
			permissions: []string{allTimeLoc},
			want:        true,
		},
		{
			name:        "coordinates denied with only non-location",
			signal:      "currentLocationCoordinates",
			permissions: []string{nonLoc},
			want:        false,
		},
		{
			name:        "altitude denied with only non-location",
			signal:      "currentLocationAltitude",
			permissions: []string{nonLoc},
			want:        false,
		},
		{
			name:        "altitude allowed with all-time location",
			signal:      "currentLocationAltitude",
			permissions: []string{allTimeLoc},
			want:        true,
		},
		{
			name:        "heading denied with only non-location",
			signal:      "currentLocationHeading",
			permissions: []string{nonLoc},
			want:        false,
		},
		{
			name:        "heading allowed with all-time location",
			signal:      "currentLocationHeading",
			permissions: []string{allTimeLoc},
			want:        true,
		},
		{
			name:        "approximate allowed with approximate permission",
			signal:      model.ApproximateCoordinatesField,
			permissions: []string{approxLoc},
			want:        true,
		},
		{
			name:        "approximate allowed with all-time location",
			signal:      model.ApproximateCoordinatesField,
			permissions: []string{allTimeLoc},
			want:        true,
		},
		{
			name:        "approximate denied with only non-location",
			signal:      model.ApproximateCoordinatesField,
			permissions: []string{nonLoc},
			want:        false,
		},
		{
			name:        "non-location signal allowed with non-location permission",
			signal:      "speed",
			permissions: []string{nonLoc},
			want:        true,
		},
		{
			name:        "non-location signal denied with no permissions",
			signal:      "speed",
			permissions: nil,
			want:        false,
		},
		{
			name:        "unknown signal denied even with all permissions",
			signal:      "notARealSignalName",
			permissions: []string{nonLoc, allTimeLoc, approxLoc},
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			holds := func(a string) bool { return slices.Contains(tt.permissions, a) }
			got := hasPrivilegesForSignal(repo, tt.signal, holds)
			if got != tt.want {
				t.Errorf("hasPrivilegesForSignal(%q, %v) = %v, want %v",
					tt.signal, tt.permissions, got, tt.want)
			}
		})
	}
}
