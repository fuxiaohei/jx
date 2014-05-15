package gojx

import (
	"crypto/rand"
	"fmt"
	"github.com/Unknwon/com"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

var (
	s          *Storage
	insertSize = 100
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
	Id       int    `jx:"pk"`
	UserName string `jx:"index"`
	Password string
	Email    string `jx:"index"`
}

type School struct {
	Id      int `jx:"pk"`
	Address string
	Rank    int `jx:"index"`
}

type Student struct {
	No    int `jx:"pk"`
	Name  string
	Class string `jx:"index"`
	Grade string `jx:"index"`
}

func init() {
	os.RemoveAll(directory)
	s, _ = NewStorage(directory, MAPPER_JSON)
	s.Register(new(User), 20)
	s.Register(new(School), 100)
	for i := 0; i <= insertSize; i++ {
		user := new(User)
		user.UserName = randomString(3)
		user.Email = randomString(12)
		user.Password = randomString(10)
		s.Put(user)
		if i%3 == 1 {
			school := new(School)
			school.Address = randomString(20)
			school.Rank = randomInt(4)
			s.Put(school)
		}
	}
}

func printError(t *testing.T, name string, except, got interface{}) {
	t.Errorf("%s: (Except => %v) (Got => %v)", name, except, got)
}

func TestRegister(t *testing.T) {
	e := s.Register(new(Student), 55)
	if e != nil {
		t.Error(e)
		return
	}
	name := strings.ToLower(fmt.Sprint(reflect.TypeOf(new(Student)).Elem()))
	sc := s.schema[name]
	if sc == nil {
		printError(t, "RegisterNoSchema", name, nil)
		return
	}
	if sc.PK != "No" {
		printError(t, "RegisterPk", "No", sc.PK)
		return
	}

	if sc.Index[1] != "Grade" {
		printError(t, "RegisterIndexHas", "Grade", sc.Index[1])
		return
	}

	file := path.Join(directory, name, "grade.idx")
	if !com.IsFile(file) {
		printError(t, "RegisterHasFile:"+file, true, false)
		return
	}

	if sc.ChunkSize != 55 {
		printError(t, "RegisterChunkSize", 55, sc.ChunkSize)
	}

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
	if user.Id != 102 {
		printError(t, "Put", 102, user.Id)
		return
	}

	user = new(User)
	user.Id = 999
	user.UserName = randomString(3)
	user.Email = randomString(12)
	user.Password = randomString(10)
	e = s.Put(user)
	if e != nil {
		t.Error(e)
		return
	}
	name := strings.ToLower(fmt.Sprint(reflect.TypeOf(user).Elem()))
	sc := s.schema[name]
	if sc.Max != 999 {
		printError(t, "PutOverMax", 999, sc.Max)
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
	if user.Id != 1000 {
		printError(t, "PutAfterOverMax", 1000, user.Id)
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
		printError(t, "GetByPk", u.UserName+":"+u.Email, nil)
		return
	}
}

func BenchmarkGetByPk(b *testing.B) {
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
	e := s.Update(user)
	if e != ErrorNoData {
		printError(t, "UpdateNonExist", ErrorNoData, e)
		return
	}

	user.Id = 9
	e = s.Update(user)
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
		s.Update(user)
	}
}

func TestDelete(t *testing.T) {
	user := new(User)
	user.Id = 9999
	e := s.Delete(user)
	if e != ErrorNoData {
		printError(t, "DeleteNonExist", ErrorNoData, e)
		return
	}

	user.Id = 9
	e = s.Delete(user)
	if e != nil {
		t.Error(e)
		return
	}
	e = s.Get(user)
	if e != ErrorNoData {
		printError(t, "Delete", ErrorNoData, e)
		return
	}
}

func BenchmarkDelete(b *testing.B) {
	user := new(User)
	user.Id = 0
	for i := 0; i <= b.N; i++ {
		user.Id = i + 1
		s.Delete(user)
	}
}
