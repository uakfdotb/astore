package main

type Template struct {
	ArchiveURL string
	ServerCommand string
	ClientCommand string
}

func getTemplates() map[string]*Template {
	templates := make(map[string]*Template)

	templates["ldr"] = &Template{
		ArchiveURL: "http://172.81.176.82/archive.zip",
		ServerCommand: "/home/ubuntu/code2 -algorithm ldr -mode server -port 9224",
		ClientCommand: "/home/ubuntu/code2 -algorithm ldr -mode bench",
	}

	templates["cas"] = &Template{
		ArchiveURL: "http://172.81.176.82/archive.zip",
		ServerCommand: "/home/ubuntu/code2 -algorithm cas -mode server -port 9224",
		ClientCommand: "/home/ubuntu/code2 -algorithm cas -mode bench",
	}

	return templates
}
