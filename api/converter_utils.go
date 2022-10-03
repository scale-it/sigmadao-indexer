package api

const (
	addrRoleSender   = "sender"
	addrRoleReceiver = "receiver"
	addrRoleFreeze   = "freeze-target"
)

var addressRoleEnumMap = map[string]bool{
	addrRoleSender:   true,
	addrRoleReceiver: true,
	addrRoleFreeze:   true,
}
