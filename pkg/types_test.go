package stargate_test

import (
	"testing"

	"github.com/ipfs/stargate/internal/testutil"
	stargate "github.com/ipfs/stargate/pkg"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/stretchr/testify/require"
)

func TestStarGateMessageRoundtrip(t *testing.T) {
	testCases := []struct {
		name            string
		starGateMessage stargate.StarGateMessage
	}{
		{
			name: "Path Message",
			starGateMessage: stargate.StarGateMessage{
				Kind: stargate.KindPath,
				Path: &stargate.Path{
					Segments: []string{"apples", "orages"},
					Blocks: stargate.BlockMetadata{
						{
							Link:   testutil.GenerateCid(),
							Status: stargate.BlockStatusPresent,
						},
						{
							Link:   testutil.GenerateCid(),
							Status: stargate.BlockStatusNotSent,
						},
					},
				},
			},
		},
		{
			name: "Path Message",
			starGateMessage: stargate.StarGateMessage{
				Kind: stargate.KindDAG,
				DAG: &stargate.DAG{
					Ordering: stargate.OrderingBreadthFirst,
					Blocks: stargate.BlockMetadata{
						{
							Link:   testutil.GenerateCid(),
							Status: stargate.BlockStatusPresent,
						},
						{
							Link:   testutil.GenerateCid(),
							Status: stargate.BlockStatusNotSent,
						},
					},
				},
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			data, err := stargate.BindnodeRegistry.TypeToBytes(&testCase.starGateMessage, dagcbor.Encode)
			require.NoError(t, err)
			result, err := stargate.BindnodeRegistry.TypeFromBytes(data, (*stargate.StarGateMessage)(nil), dagcbor.Decode)
			require.NoError(t, err)
			require.Equal(t, &testCase.starGateMessage, result)
		})
	}
}
