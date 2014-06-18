#jx


## jx - a simple json storage engine by Golang.

jx is a storage engine use simple json by kv map encoded as documentation. It provides simple api methods to operate value in storage.
It can be used as *embedded* storage.

### Getting Started

`jx` saves real data in files. so create a `*jx.Storage` in directory.

    import "github.com/fuxiaohei/jx"
    
    s,e := jx.NewStorage("data")
    if e != nil{
        panic(e) // remember error
    }

It creates storage directory for saving data. *But* it doesn't load existing data now.

#### 1. Sync

Then sync a struct to storage, so storage can save the struct value.


    type User struct {
        Id       int64    `jx:"pk-auto"`
        UserName string 
        Password string
        Email    string 
    }
    
    type School struct {
        Id      int64 `jx:"pk"`
        Address string
        Rank    int 
    }
    
    e := s.Sync(new(User),new(School))

`s.Sync(...)` only support struct pointer type.

`jx:"pk"` means primary key for this field, only support **int64, string, float64** type.

`jx:"pk-auto"` means auto-increment primary key for this field, only support **int64** type.Insert

The pk field is unique, so if two "pk" tag in struct, the last one is assigned.

**Notice:**

When sync a struct, it creates default data files if not exist yet.

Or it reads old data for memory as preparation.

#### 2. Insert

Insert a new struct value into storage:

    u := new(User)
    u.UserName = "abcdef"
    u.Password = "12345678"
    u.Email = "abcdef@xyz.com"
    
    e := s.Put(u) // u.Id is auto increasing.
    

**Insert** only support **struct pointer**.

The pk field `u.Id` is assigned as max id in storage and increasing one by one.

If `pk-auto` tag, no matter the pk you set in struct, use storage max pk int.

    s := new(School)
    s.Id = 123
    s.Address = "address"
    S.Rank = 4
    
    e := s.Put(s) // pk field 
    if e == jx.Conflict{
        // means the pk is used in storage.
    }
    if e == jx.Wrong{
        // means the pk field is wrong value, type error and empty
    }

When `s.Insert(u)`, it checks pk field value unique. 

Then write data to chunk files.


#### 3. Get

Now only support get value by pk field 


    u := &User{Id:100}
    e := s.Get(u)
    if e == jx.Nil{
        println("get no data")
    }else{
        println(u.UserName) // if found, field is filled.
    }


**Get** only support **struct pointer** and by **pk field**.

If value is not synced, return error.

If value is found, `u` is filled by value.

##### 4. Update

Only support update value by pk field:

    u := new(User)
    u.Id = 1
    u.UserName = "mnopq"
    u.Password = "9876543"
    u.Email = "xyz@abc.com"
    
    e := s.Update(u)


##### 5. Delete

Delete value by pk:

    u := new(User)
    u.Id = 1
    
    e := s.Delete(u)

