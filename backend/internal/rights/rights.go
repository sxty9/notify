// Package rights holds notify's hp_* group constants — kept 1:1 with permissions/notify.json
// and the shell's use of the service. Notifications are inherently per-user (you always see
// your own), so the only gated capability is the admin one: broadcasting to everyone.
package rights

// GroupAdmin backs notify:admin — broadcast a notification to all users and manage the service.
const GroupAdmin = "hp_notify_admin"
