package paxos

import (
	"fmt"
	pb "github.com/AllenShaw19/paxoskv/proto"
	"math"
)

const (
	kInvalidAcceptorId uint32 = math.MaxUint32
	kInvalidEntry      uint32 = math.MaxUint32
)

func EntryRecordToString(e *pb.EntryRecord) string {
	buffer := fmt.Sprintf("%d %d %d vid %d u.sz %d v.sz %d cho %v has %v",
		e.PreparedNum,
		e.PromisedNum,
		e.AcceptedNum,
		e.ValueId,
		len(e.Uuids),
		len(e.Value),
		e.Chosen,
		e.HasValueIdOnly)
	return buffer
}
