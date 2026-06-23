package duck

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWhereClauseTagsAndDataVersion(t *testing.T) {
	where, args := whereClause(RawFilter{
		Subject:      "did:1",
		DataVersions: []string{"v1"},
		Tags:         []string{"a", "b"},
	})
	require.Contains(t, where, "data_version IN")
	require.Contains(t, where, "list_has_any") // tags JSON array overlap
	require.Contains(t, args, "did:1")
	require.Contains(t, args, "v1")
	require.Contains(t, args, "a")
	require.Contains(t, args, "b")
}

func TestWhereClauseQ_PrefixQualifiesColumns(t *testing.T) {
	where, args := whereClauseQ(RawFilter{
		Subject: "did:subj",
		Types:   []string{"dimo.status"},
	}, "e.")
	require.Contains(t, where, "e.subject = ?")
	require.Contains(t, where, "e.type IN")
	require.Contains(t, args, "did:subj")
	require.Contains(t, args, "dimo.status")
}
