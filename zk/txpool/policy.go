package txpool

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/log/v3"
)

// Policy is a named policy
type Policy byte

// when a new Policy is added, it should be added to policiesList also.
const (
	// SendTx is the name of the policy that governs that an address may send transactions to pool
	SendTx Policy = iota
	// Deploy is the name of the policy that governs that an address may deploy a contract
	Deploy
)

var policiesList = []Policy{SendTx, Deploy}

func (p Policy) ToByte() byte {
	return byte(p)
}

func (p Policy) ToByteArray() []byte {
	return []byte{byte(p)}
}

// IsSupportedPolicy checks if the given policy is supported
func IsSupportedPolicy(policy Policy) bool {
	switch policy {
	case SendTx, Deploy:
		return true
	default:
		return false
	}
}

func ResolvePolicy(policy string) (Policy, error) {
	switch policy {
	case "sendTx":
		return SendTx, nil
	case "deploy":
		return Deploy, nil
	default:
		return SendTx, errUnknownPolicy
	}
}

// containsPolicy checks if the given policy is present in the policy list
func containsPolicy(policies []byte, policy Policy) bool {
	return bytes.Contains(policies, policy.ToByteArray())
}

// address policyMapping returns a string of user policies.
func policyMapping(policies []byte, pList []Policy) string {
	policyPresence := make(map[string]bool)

	for _, policy := range pList {
		// Check if the policy exists in the provided byte slice
		exists := bytes.Contains(policies, policy.ToByteArray())
		if policyName(policy) == "unknown" {
			continue
		}
		// Store the result in the map with the policy name
		policyPresence[policyName(policy)] = exists
	}

	// could be used to return a map here

	// Create a slice to hold the formatted policy strings
	formattedPolicies := make([]string, 0, len(policyPresence))

	// Populate the slice with formatted strings
	for policy, exists := range policyPresence {
		formattedPolicies = append(formattedPolicies, fmt.Sprintf("\t%s: %v", policy, exists))
	}

	// Join the formatted strings with ", "
	return strings.Join(formattedPolicies, "\n")
}

// policyName returns the string name of a policy
func policyName(policy Policy) string {
	switch policy {
	case SendTx:
		return "sendTx"
	case Deploy:
		return "deploy"
	default:
		return "unknown"
	}
}

// DoesAccountHavePolicy checks if the given account has the given policy for the online ACL mode
func DoesAccountHavePolicy(ctx context.Context, aclDB kv.RwDB, addr common.Address, policy Policy) (bool, error) {
	hasPolicy, _, err := checkIfAccountHasPolicy(ctx, aclDB, addr, policy)
	return hasPolicy, err
}

func checkIfAccountHasPolicy(ctx context.Context, aclDB kv.RwDB, addr common.Address, policy Policy) (bool, ACLMode, error) {
	if !IsSupportedPolicy(policy) {
		return false, DisabledMode, errUnknownPolicy
	}

	// Retrieve the mode configuration
	var (
		hasPolicy bool
		mode      ACLMode = DisabledMode
	)

	err := aclDB.View(ctx, func(tx kv.Tx) error {
		value, err := tx.GetOne(Config, []byte("mode"))
		if err != nil {
			return err
		}

		if value == nil || string(value) == DisabledMode {
			hasPolicy = true
			return nil
		}

		mode = ACLMode(value)

		table := BlockList
		if mode == AllowlistMode {
			table = Allowlist
		}

		var policyBytes []byte
		value, err = tx.GetOne(table, addr.Bytes())
		if err != nil {
			return err
		}

		policyBytes = value
		if policyBytes != nil && containsPolicy(policyBytes, policy) {
			// If address is in the allowlist and has the policy, return true
			// If address is in the blocklist and has the policy, return false
			hasPolicy = true
		}

		return nil
	})
	if err != nil {
		return false, mode, err
	}

	return hasPolicy, mode, nil
}

// UpdatePolicies sets a policy for an address
func UpdatePolicies(ctx context.Context, aclDB kv.RwDB, aclType string, addrs []common.Address, policies [][]Policy) error {
	table, err := resolveTable(aclType)
	if err != nil {
		return err
	}

	return aclDB.Update(ctx, func(tx kv.RwTx) error {
		for i, addr := range addrs {
			if len(policies[i]) > 0 {
				// just update the policies for the address to match the one provided
				policyBytes := make([]byte, 0, len(policies[i]))
				for _, p := range policies[i] {
					policyBytes = append(policyBytes, p.ToByte())
				}

				if err := tx.Put(table, addr.Bytes(), policyBytes); err != nil {
					return err
				}

				continue
			}

			// remove the address from the table
			if err := tx.Delete(table, addr.Bytes()); err != nil {
				return err
			}
		}

		return nil
	})
}

// AddPolicy adds a policy to the ACL of given address
func AddPolicy(ctx context.Context, aclDB kv.RwDB, aclType string, addr common.Address, policy Policy) error {
	if !IsSupportedPolicy(policy) {
		return errUnknownPolicy
	}

	table, err := resolveTable(aclType)
	if err != nil {
		return err
	}

	return aclDB.Update(ctx, func(tx kv.RwTx) error {
		value, err := tx.GetOne(table, addr.Bytes())
		if err != nil {
			return err
		}

		policyBytes := policy.ToByteArray()
		if value == nil {
			return tx.Put(table, addr.Bytes(), policyBytes)
		}

		// Check if the policy already exists
		if containsPolicy(value, policy) {
			return nil
		}

		value = append(value, policyBytes...)

		return tx.Put(table, addr.Bytes(), value)
	})
}

// RemovePolicy removes a policy from the ACL of given address
func RemovePolicy(ctx context.Context, aclDB kv.RwDB, aclType string, addr common.Address, policy Policy) error {
	table, err := resolveTable(aclType)
	if err != nil {
		return err
	}

	return aclDB.Update(ctx, func(tx kv.RwTx) error {
		policies, err := tx.GetOne(table, addr.Bytes())
		if err != nil {
			return err
		}
		if policies == nil {
			// No policies exist for this address
			return nil
		}

		updatedPolicies := []byte{}

		for _, p := range policies {
			if p != policy.ToByte() {
				updatedPolicies = append(updatedPolicies, p)
			}
		}

		if len(updatedPolicies) == 0 {
			return tx.Delete(table, addr.Bytes())
		}

		return tx.Put(table, addr.Bytes(), updatedPolicies)
	})
}

func ListContentAtACL(ctx context.Context, db kv.RwDB) error {

	var buffer bytes.Buffer

	tables := db.AllTables()
	buffer.WriteString("ListContentAtACL\n")
	buffer.WriteString("Tables\nTable - { Flags, AutoDupSortKeysConversion, IsDeprecated, DBI, DupFromLen, DupToLen }\n")
	for key, config := range tables {
		buffer.WriteString(fmt.Sprint(key, config, "\n"))
	}

	err := db.View(ctx, func(tx kv.Tx) error {
		// Config table
		buffer.WriteString("\nConfig\n")
		err := tx.ForEach(Config, nil, func(k, v []byte) error {
			buffer.WriteString(fmt.Sprintf("Key: %s, Value: %s\n", string(k), string(v)))
			return nil
		})

		// BlockList table
		var BlockListContent strings.Builder
		err = tx.ForEach(BlockList, nil, func(k, v []byte) error {
			BlockListContent.WriteString(fmt.Sprintf(
				"Key: %s, Value: {\n%s\n}\n",
				hex.EncodeToString(k),
				policyMapping(v, policiesList),
			))
			return nil
		})
		if err != nil {
			return err
		}
		if BlockListContent.String() != "" {
			buffer.WriteString(fmt.Sprintf(
				"\nBlocklist\n%s",
				BlockListContent.String(),
			))
		} else {
			buffer.WriteString("\nBlocklist is empty")
		}

		// Allowlist table
		var AllowlistContent strings.Builder
		err = tx.ForEach(Allowlist, nil, func(k, v []byte) error {
			AllowlistContent.WriteString(fmt.Sprintf(
				"Key: %s, Value: {\n%s\n}\n",
				hex.EncodeToString(k),
				policyMapping(v, policiesList),
			))
			return nil
		})
		if err != nil {
			return err
		}
		if AllowlistContent.String() != "" {
			buffer.WriteString(fmt.Sprintf(
				"\nAllowlist\n%s",
				AllowlistContent.String(),
			))
		} else {
			buffer.WriteString("\nAllowlist is empty")
		}

		return err
	})
	if err == nil {
		fmt.Println(buffer.String())
		log.Info("ACL content", "content", buffer.String())
	}

	return err
}

// SetMode sets the mode of the ACL
func SetMode(ctx context.Context, aclDB kv.RwDB, mode string) error {
	m, err := ResolveACLMode(mode)
	if err != nil {
		return err
	}

	return aclDB.Update(ctx, func(tx kv.RwTx) error {
		return tx.Put(Config, []byte(modeKey), []byte(m))
	})
}

// GetMode gets the mode of the ACL
func GetMode(ctx context.Context, aclDB kv.RwDB) (ACLMode, error) {
	var mode ACLMode
	err := aclDB.View(ctx, func(tx kv.Tx) error {
		value, err := tx.GetOne(Config, []byte(modeKey))
		if err != nil {
			return err
		}

		mode = ACLMode(value)
		return nil
	})

	return mode, err
}

// resolveTable resolves the ACL table based on aclType
func resolveTable(aclType string) (string, error) {
	at, err := ResolveACLType(aclType)
	if err != nil {
		return "", err
	}

	table := BlockList
	if at == AllowListType {
		table = Allowlist
	}

	return table, nil
}

// create a method to resolve policy which will decode a tx to either sendTx or deploy policy
func resolvePolicy(txn *types.TxSlot) Policy {
	if txn.Creation {
		return Deploy
	}
	return SendTx
}

// isActionAllowed checks if the given action is allowed for the given address
func (p *TxPool) isActionAllowed(ctx context.Context, addr common.Address, policy Policy) (bool, error) {
	hasPolicy, mode, err := checkIfAccountHasPolicy(ctx, p.aclDB, addr, policy)
	if err != nil {
		return false, err
	}

	switch mode {
	case BlocklistMode:
		// If the mode is blocklist, and address has a certain policy, then invert the result
		// because, for example, if it has sendTx policy, it means it is not allowed to sendTx
		return !hasPolicy, nil
	default:
		return hasPolicy, nil
	}
}
