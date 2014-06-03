package gojx

import (
	"crypto/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
)

var (
	s          *Storage
	insertSize = 1666
	directory  = "_test"
)

func randomString(n int) string {
	const letters = "abc"
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = letters[b%byte(len(letters))]
	}
	return string(bytes)
}

func randomInt(n int) int {
	const nums = "0123456789"
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = nums[b%byte(len(nums))]
	}
	res, _ := strconv.Atoi(string(bytes))
	return res
}

type User struct {
	Id       int64  `gojx:"pk"`
	UserName string `gojx:"index"`
	Password string
	Email    string `gojx:"index"`
}

type School struct {
	Id      int64 `gojx:"pk"`
	Address string
	Rank    int `gojx:"index"`
}

type Student struct {
	No    int64 `gojx:"pk"`
	Name  string
	Class string `gojx:"index"`
	Grade string `gojx:"index"`
}

func init() {
	os.RemoveAll(directory)

	var e error
	s, e = New(StorageConfig{Dir: directory, Encoder: new(GobEncoder)})
	if e != nil {
		panic(e)
	}
	e = s.Sync(new(User))
	if e != nil {
		panic(e)
	}

	for i := 0; i < insertSize; i++ {
		user := new(User)
		user.UserName = randomString(3)
		user.Email = randomString(12)
		user.Password = randomString(10)
		if e = s.Put(user); e != nil {
			panic(e)
		}
	}

	e = s.Flush()
	if e != nil {
		panic(e)
	}

	// reload
	s, e = New(StorageConfig{Dir: directory, Encoder: new(GobEncoder)})
	if e != nil {
		panic(e)
	}
	e = s.Sync(new(User))
	if e != nil {
		panic(e)
	}
}

func printError(t *testing.T, name string, except, got interface{}) {
	t.Errorf("%s: (Except => %v) (Got => %v)", name, except, got)
}

func TestPut(t *testing.T) {
	user := new(User)
	user.UserName = randomString(3)
	user.Email = randomString(12)
	user.Password = randomString(10)
	e := s.Put(user)
	if e != nil {
		t.Error(e)
		return
	}
	if user.Id != 1667 {
		printError(t, "Put", 1667, user.Id)
		return
	}

	user = new(User)
	user.Id = 1999
	user.UserName = randomString(3)
	user.Email = randomString(12)
	user.Password = randomString(10)
	e = s.Put(user)
	if e != nil {
		t.Error(e)
		return
	}
	obj := s.Objects[reflect.TypeOf(User{}).String()]
	if obj.Increment != 1999 {
		printError(t, "PutOverMax", 1999, obj.Increment)
		return
	}

	user = new(User)
	user.UserName = randomString(3)
	user.Email = randomString(12)
	user.Password = randomString(10)
	e = s.Put(user)
	if e != nil {
		t.Error(e)
		return
	}
	if user.Id != 2000 {
		printError(t, "PutAfterOverMax", 2000, user.Id)
		return
	}
}

func BenchmarkPut(b *testing.B) {
	user := new(User)
	user.UserName = randomString(3)
	user.Email = randomString(12)
	user.Password = randomString(10)
	for i := 0; i < b.N; i++ {
		s.Put(user)
	}
}

func TestGet(t *testing.T) {
	u := &User{Id: 100}
	e := s.Get(u)
	if e != nil {
		t.Error(e)
		return
	}
	if len(u.UserName) != 3 || len(u.Email) != 12 {
		printError(t, "Get", u.UserName+":"+u.Email, nil)
		return
	}
}

func BenchmarkGet(b *testing.B) {
	u := &User{Id: 99}
	for i := 0; i < b.N; i++ {
		s.Get(u)
	}
}

func TestUpdate(t *testing.T) {
	user := new(User)
	user.Id = 9999
	user.UserName = "aaa"
	user.Email = randomString(12)
	user.Password = randomString(10)
	e := s.Set(user)
	if e != NULL {
		printError(t, "UpdateNonExist", NULL, e)
		return
	}

	user.Id = 9
	e = s.Set(user)
	if e != nil {
		printError(t, "Update", nil, e)
		return
	}

	user2 := new(User)
	user2.Id = 9
	e = s.Get(user2)
	if e != nil {
		t.Error(e)
		return
	}
	if user2.UserName != "aaa" {
		printError(t, "UpdateThenGet", "aaa", user2.UserName)
		return
	}
}

func BenchmarkUpdate(b *testing.B) {
	user := new(User)
	user.Id = 99
	user.UserName = randomString(3)
	user.Email = randomString(12)
	user.Password = randomString(10)
	for i := 0; i < b.N; i++ {
		s.Set(user)
	}
}

func TestDelete(t *testing.T) {
	user := new(User)
	user.Id = 9999
	e := s.Del(user)
	if e != NULL {
		printError(t, "DeleteNonExist", NULL, e)
		return
	}

	user.Id = 9
	e = s.Del(user)
	if e != nil {
		t.Error(e)
		return
	}
	e = s.Get(user)
	if e != NULL {
		printError(t, "Delete", NULL, e)
		return
	}
}

func BenchmarkDelete(b *testing.B) {
	user := new(User)
	user.Id = 0
	for i := 0; i <= b.N; i++ {
		user.Id = int64(i + 1)
		s.Del(user)
	}
}
