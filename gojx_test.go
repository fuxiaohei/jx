package gojx

import (
	"os"
	"testing"
)

var (
	s          *Storage
	insertUser *user
)

type user struct {
	Id       int    `jx:"pk"`
	UserName string `jx:"index"`
	Password string
	Email    string `jx:"index"`
}

func s_init(ins bool) {
	os.RemoveAll("test")
	insertUser = &user{0, "username", "password", "email"}
	initStorage()
	if ins {
		insert()
	}
}

func insert() {
	for i := 0; i < 999; i++ {
		s.Put(insertUser)
	}
}

func initStorage() {
	s = nil
	s, _ = NewStorage("test")
	s.Register(new(user))
}

func BenchmarkInsertData(b *testing.B) {
	b.StopTimer()
	s_init(false)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s.Put(insertUser)
	}
}

func BenchmarkGetPkInDelayLoad(b *testing.B) {
	b.StopTimer()
	s_init(true)
	initStorage()
	b.StartTimer()
	u := &user{Id: 9}
	for i := 0; i < b.N; i++ {
		s.Get(u)
	}
}

func BenchmarkGetPkInPreLoad(b *testing.B) {
	b.StopTimer()
	s_init(true)
	initStorage()
	b.StartTimer()
	u := &user{Id: 911}
	for i := 0; i < b.N; i++ {
		s.Get(u)
	}
}
