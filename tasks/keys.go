package tasks

func catalogKey(ns string) string {
	return "srq:tasks:" + ns + ":catalog"
}

func runKey(ns, name string) string {
	return "srq:tasks:" + ns + ":run:" + name
}

func namespacesKey() string {
	return "srq:tasks:namespaces"
}
