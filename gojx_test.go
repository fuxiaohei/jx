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
	Id       int    `gojx:"pk"`
	UserName string `gojx:"index"`
	Password string
	Email    string `gojx:"index"`
}

func my_init() {
	os.RemoveAll("test")
	s, _ = NewStorage("test")
	s.Register(new(user))
	insertUser = &user{0, "username", "password", "email"}
}

func insert() {
	for i := 0; i < 888; i++ {
		s.Insert(insertUser)
	}
}

func BenchmarkInsertData(b *testing.B) {
	b.StopTimer()
	my_init()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s.Insert(insertUser)
	}
}

func BenchmarkGetPkInDelayLoad(b *testing.B) {
	b.StopTimer()
	my_init()
	insert()
	b.StartTimer()
	u := &user{Id:9}
	for i := 0; i < b.N; i++ {
		s.Get(u)
	}
}

func BenchmarkGetPkInPreLoad(b *testing.B) {
	b.StopTimer()
	my_init()
	insert()
	b.StartTimer()
	u := &user{Id:911}
	for i := 0; i < b.N; i++ {
		s.Get(u)
	}
}

