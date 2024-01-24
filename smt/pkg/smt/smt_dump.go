package smt

import (
	"fmt"
	"strconv"

	"github.com/ledgerwatch/erigon/smt/pkg/utils"
)

var KeyPointers = []*utils.NodeKey{}
var ValuePointers = []*utils.NodeValue8{}

func (s *SMT) DumpTree() {
	rootNodeKey, _ := s.getLastRoot()
	dumpTree(s, rootNodeKey, 0, []int{}, 12)
}

func dumpTree(smt *SMT, nodeKey utils.NodeKey, level int, path []int, printDepth int) {
	if nodeKey.IsZero() {
		if level == 0 {
			fmt.Printf("Empty tree\n")
		}
		return
	}

	nodeValue, _ := smt.Db.Get(nodeKey)
	if !nodeValue.IsFinalNode() {
		nodeKeyRight := utils.NodeKeyFromBigIntArray(nodeValue[4:8])
		dumpTree(smt, nodeKeyRight, level+1, append(path, 1), printDepth)
	}

	if nodeValue.IsFinalNode() {
		rKey := utils.NodeKeyFromBigIntArray(nodeValue[0:4])
		// rKeyPath := rKey.GetPath()
		leafValueHash := utils.NodeKeyFromBigIntArray(nodeValue[4:8])
		totalKey := utils.JoinKey(path, rKey)
		leafPath := totalKey.GetPath()
		// if len(rKeyPath) != 256 {
		// 	fmt.Println()
		// }
		fmt.Printf("|")
		for i := 0; i < level; i++ {
			fmt.Printf("=")
		}
		fmt.Printf("%s", convertPathToBinaryString(path))
		for i := level * 2; i < printDepth; i++ {
			fmt.Printf("-")
		}
		fmt.Printf(" # %s -> %+v rKey(%+v) hash(%s)", convertPathToBinaryString(leafPath), leafValueHash, rKey, utils.ConvertBigIntToHex(utils.ArrayToScalar(nodeKey[:])))
		fmt.Println()
		return
	} else {
		fmt.Printf("|")
		for i := 0; i < level; i++ {
			fmt.Printf("=")
		}
		fmt.Printf("%s", convertPathToBinaryString(path))
		for i := level * 2; i < printDepth; i++ {
			fmt.Printf("-")
		}
		fmt.Printf(" # hashLeft(%s) <-> hashRight(%s)", utils.ConvertBigIntToHex(utils.ArrayBigToScalar(nodeValue[0:4])), utils.ConvertBigIntToHex(utils.ArrayBigToScalar(nodeValue[4:8])))
		fmt.Println()
	}

	if !nodeValue.IsFinalNode() {
		nodeKeyLeft := utils.NodeKeyFromBigIntArray(nodeValue[0:4])
		dumpTree(smt, nodeKeyLeft, level+1, append(path, 0), printDepth)
	}
}

func convertPathToBinaryString(path []int) string {
	out := ""

	size := len(path)
	if size > 8 {
		size = 8
	}

	for i := 0; i < size; i++ {
		out += strconv.Itoa(path[i])
	}

	return out
}
