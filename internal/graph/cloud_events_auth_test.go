package graph

import (
	"context"
	"testing"

	"github.com/DIMO-Network/dq/internal/graph/model"
	"github.com/DIMO-Network/token-exchange-api/pkg/tokenclaims"
)

const (
	testSource = "0xAAaaAAaaAAaaAAaaAAaaAAaaAAaaAAaaAAaaAAaa"
	otherSrc   = "0xBBbbBBbbBBbbBBbbBBbbBBbbBBbbBBbbBBbbBBbb"
	idOne      = "id-1"
	idTwo      = "id-2"
	idThree    = "id-3"
)

func strPtr(s string) *string { return &s }

func TestCloudEventRequestAllowed(t *testing.T) {
	wildcard := &tokenclaims.CloudEvents{Events: []tokenclaims.Event{{
		EventType: tokenclaims.GlobalIdentifier,
		Source:    tokenclaims.GlobalIdentifier,
		IDs:       []string{tokenclaims.GlobalIdentifier},
	}}}
	attFromSource := &tokenclaims.CloudEvents{Events: []tokenclaims.Event{{
		EventType: "dimo.attestation",
		Source:    testSource,
		IDs:       []string{tokenclaims.GlobalIdentifier},
	}}}
	specificIDs := &tokenclaims.CloudEvents{Events: []tokenclaims.Event{{
		EventType: "dimo.attestation",
		Source:    testSource,
		IDs:       []string{idOne, idTwo},
	}}}

	tests := []struct {
		name   string
		grants *tokenclaims.CloudEvents
		filter *model.CloudEventFilter
		want   bool
	}{
		{
			name:   "nil grants denied",
			grants: nil,
			filter: &model.CloudEventFilter{Type: strPtr("dimo.attestation")},
			want:   false,
		},
		{
			name:   "empty events denied",
			grants: &tokenclaims.CloudEvents{},
			filter: nil,
			want:   false,
		},
		{
			name:   "wildcard grant, no filter, allowed",
			grants: wildcard,
			filter: nil,
			want:   true,
		},
		{
			name:   "wildcard grant, specific filter, allowed",
			grants: wildcard,
			filter: &model.CloudEventFilter{Type: strPtr("dimo.status"), Source: strPtr(otherSrc), ID: strPtr(idThree)},
			want:   true,
		},
		{
			name:   "narrow grant, matching filter, allowed",
			grants: attFromSource,
			filter: &model.CloudEventFilter{Type: strPtr("dimo.attestation"), Source: strPtr(testSource)},
			want:   true,
		},
		{
			name:   "narrow grant, no filter, denied (defaults to wildcards)",
			grants: attFromSource,
			filter: nil,
			want:   false,
		},
		{
			name:   "narrow grant, missing source filter, denied",
			grants: attFromSource,
			filter: &model.CloudEventFilter{Type: strPtr("dimo.attestation")},
			want:   false,
		},
		{
			name:   "narrow grant, wrong source, denied",
			grants: attFromSource,
			filter: &model.CloudEventFilter{Type: strPtr("dimo.attestation"), Source: strPtr(otherSrc)},
			want:   false,
		},
		{
			name:   "narrow grant, wrong type, denied",
			grants: attFromSource,
			filter: &model.CloudEventFilter{Type: strPtr("dimo.status"), Source: strPtr(testSource)},
			want:   false,
		},
		{
			name:   "specific ids, granted id, allowed",
			grants: specificIDs,
			filter: &model.CloudEventFilter{Type: strPtr("dimo.attestation"), Source: strPtr(testSource), ID: strPtr(idOne)},
			want:   true,
		},
		{
			name:   "specific ids, ungranted id, denied",
			grants: specificIDs,
			filter: &model.CloudEventFilter{Type: strPtr("dimo.attestation"), Source: strPtr(testSource), ID: strPtr(idThree)},
			want:   false,
		},
		{
			name:   "specific ids, no id filter, denied (defaults to wildcard id)",
			grants: specificIDs,
			filter: &model.CloudEventFilter{Type: strPtr("dimo.attestation"), Source: strPtr(testSource)},
			want:   false,
		},
		{
			name:   "plural types, every type covered, allowed",
			grants: wildcard,
			filter: &model.CloudEventFilter{Types: []string{"dimo.attestation", "dimo.status"}},
			want:   true,
		},
		{
			name:   "plural types, one type uncovered, denied",
			grants: attFromSource,
			filter: &model.CloudEventFilter{Types: []string{"dimo.attestation", "dimo.status"}, Source: strPtr(testSource)},
			want:   false,
		},
		{
			name:   "type and types merged, all covered, allowed",
			grants: wildcard,
			filter: &model.CloudEventFilter{Type: strPtr("dimo.attestation"), Types: []string{"dimo.status"}},
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cloudEventRequestAllowed(tt.grants, tt.filter); got != tt.want {
				t.Errorf("cloudEventRequestAllowed() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRequireSubjectOptsByDID(t *testing.T) {
	const subject = "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42"

	rawDataToken := &tokenclaims.Token{CustomClaims: tokenclaims.CustomClaims{
		Asset:       subject,
		Permissions: []string{tokenclaims.PermissionGetRawData},
	}}
	locComboToken := &tokenclaims.Token{CustomClaims: tokenclaims.CustomClaims{
		Asset:       subject,
		Permissions: []string{tokenclaims.PermissionGetLocationHistory, tokenclaims.PermissionGetNonLocationHistory},
	}}
	grantOnlyToken := &tokenclaims.Token{CustomClaims: tokenclaims.CustomClaims{
		Asset: subject,
		CloudEvents: &tokenclaims.CloudEvents{Events: []tokenclaims.Event{{
			EventType: "dimo.attestation",
			Source:    tokenclaims.GlobalIdentifier,
			IDs:       []string{tokenclaims.GlobalIdentifier},
		}}},
	}}
	noAccessToken := &tokenclaims.Token{CustomClaims: tokenclaims.CustomClaims{
		Asset:       subject,
		Permissions: []string{tokenclaims.PermissionExecuteCommands},
	}}

	tests := []struct {
		name    string
		token   *tokenclaims.Token
		reqDID  string
		filter  *model.CloudEventFilter
		wantErr bool
	}{
		{
			name:    "no token claims denied",
			token:   nil,
			reqDID:  subject,
			wantErr: true,
		},
		{
			name:    "raw-data token, any filter, allowed",
			token:   rawDataToken,
			reqDID:  subject,
			filter:  &model.CloudEventFilter{Type: strPtr("dimo.status")},
			wantErr: false,
		},
		{
			name:    "location combo token, no filter, allowed",
			token:   locComboToken,
			reqDID:  subject,
			wantErr: false,
		},
		{
			name:    "grant-only token, covered filter, allowed",
			token:   grantOnlyToken,
			reqDID:  subject,
			filter:  &model.CloudEventFilter{Type: strPtr("dimo.attestation")},
			wantErr: false,
		},
		{
			name:    "grant-only token, uncovered filter, denied",
			token:   grantOnlyToken,
			reqDID:  subject,
			filter:  &model.CloudEventFilter{Type: strPtr("dimo.status")},
			wantErr: true,
		},
		{
			name:    "no access token denied",
			token:   noAccessToken,
			reqDID:  subject,
			filter:  &model.CloudEventFilter{Type: strPtr("dimo.attestation")},
			wantErr: true,
		},
		{
			name:    "raw-data token, mismatched subject, denied",
			token:   rawDataToken,
			reqDID:  "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:99",
			filter:  &model.CloudEventFilter{Type: strPtr("dimo.status")},
			wantErr: true,
		},
	}

	r := &queryResolver{&Resolver{}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.token != nil {
				ctx = context.WithValue(ctx, ClaimsContextKey{}, tt.token)
			}
			_, err := r.requireSubjectOptsByDID(ctx, tt.reqDID, tt.filter)
			if (err != nil) != tt.wantErr {
				t.Errorf("requireSubjectOptsByDID() err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
