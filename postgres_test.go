package pg

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"testing"
	"time"
)

const (
	UserId = "id"

	UserParent = "parent"

	UserName = "name"

	UserCard = "card"

	UserNick = "nick"

	UserUsername = "username"

	UserPassword = "password"

	UserAvatar = "avatar"

	UserMobile = "mobile"

	UserEmail = "email"

	UserGender = "gender"

	UserAge = "age"

	UserCity = "city"

	UserLanguage = "language"

	UserIntroduce = "introduce"

	UserBalance = "balance"

	UserIntegral = "integral"

	UserCredit = "credit"

	UserGrade = "grade"

	UserStatus = "status"

	UserTime = "time"
)

type User struct {
	Id        int64   `json:"id"`
	Parent    int64   `json:"parent"`
	Name      string  `json:"name"`
	Card      string  `json:"-"`
	Nick      string  `json:"nick"`
	Username  string  `json:"username"`
	Password  string  `json:"-"`
	Avatar    string  `json:"avatar"`
	Mobile    string  `json:"mobile"`
	Email     string  `json:"email"`
	Gender    string  `json:"gender"`
	Age       int     `json:"age"`
	City      string  `json:"city"`
	Language  string  `json:"language"`
	Introduce string  `json:"introduce"`
	Balance   float64 `json:"balance"`
	Integral  float64 `json:"integral"`
	Credit    float64 `json:"credit"`
	Grade     int     `json:"grade"`
	Status    int     `json:"-"`
	Time      int64   `json:"time"`
}

func init() {
	db, err := sql.Open("postgres", "dsn")
	if err != nil {
		fmt.Println(err)
	}
	DB = db
}

var times int64 = time.Now().Unix()

func TestTable(t *testing.T) {
	// curd
	// select single condition
	user := User{}
	Table(&user).Print().WhereEqual(UserId, 100).Get(&user)
	Table(&user).Print().WhereNotEqual(UserId, 100).Get(&user)
	Table(&user).Print().WhereMoreThan(UserId, 100).Get(&user)
	Table(&user).Print().WhereMoreThanEqual(UserId, 100).Get(&user)
	Table(&user).Print().WhereLessThan(UserId, 100).Get(&user)
	Table(&user).Print().WhereLessThanEqual(UserId, 100).Get(&user)
	Table(&user).Print().WhereIn(UserId, 100, 101).Get(&user)
	Table(&user).Print().WhereNotIn(UserId, 100, 101).Get(&user)
	Table(&user).Print().WhereBetween(UserId, 100, 101).Get(&user)
	Table(&user).Print().WhereOrEqual(UserId, 100).Get(&user)
	Table(&user).Print().WhereOrNotEqual(UserId, 100).Get(&user)
	Table(&user).Print().WhereOrMoreThan(UserId, 100).Get(&user)
	Table(&user).Print().WhereOrMoreThanEqual(UserId, 100).Get(&user)
	Table(&user).Print().WhereOrLessThan(UserId, 100).Get(&user)
	Table(&user).Print().WhereOrLessThanEqual(UserId, 100).Get(&user)
	Table(&user).Print().WhereOrIn(UserId, 100, 101).Get(&user)
	Table(&user).Print().WhereOrNotIn(UserId, 100, 101).Get(&user)
	Table(&user).Print().WhereOrBetween(UserId, 100, 101).Get(&user)

	// specifying column names
	Table(&user).Print().Cols(UserId, UserName).WhereNotIn(UserId, 100, 101).Get(&user)
	Table(&user).Print().Cols(UserAvatar, UserTime).WhereOrBetween(UserId, 100, 101).Get(&user)

	// select more conditions
	Table(&user).
		Print().
		Cols(UserId, UserName).
		WhereEqual(UserId, 102).
		WhereNotIn(UserId, 100, 101).
		Group(UserId).
		Asc(UserTime).
		Get(&user)
	Table(&user).
		Print().
		WhereIn(UserId, 100, 101).
		WhereOrBracketsLeft().
		WhereEqual(UserId, 60).
		WhereOrEqual(UserId, 70).
		WhereBracketsRight().
		Get(&user)

	// winning Joint Search
	Table(&user).
		Print().
		Alias("u").
		Cols("u.id", "u.name", `sum("status") as "status"`).
		LeftJoin(&user, "v", "u.id", "v.parent").
		LeftJoin(&user, "w", "u.id", "w.parent").
		WhereBetween("u.id", 10, 100).
		WhereBracketsLeft().
		WhereMoreThanEqual("v.id", 0).
		WhereBracketsLeft().
		WhereLessThanEqual("v.id", 10000).
		WhereOrIn("v.status", 0, 1, 2, 3, 4, 5).
		WhereIn("v.status", 5, 6, 7, 8, 9).
		WhereBracketsRight().
		WhereBracketsLeft().
		WhereBetween("v.status", 0, 100).
		WhereOrMoreThanEqual("w.status", 0).
		WhereBracketsRight().
		WhereBracketsRight().
		Group("u.name").
		Desc("u.id").
		Offset(10).
		//Page(2).
		Get(&user)
	fmt.Println(user)

	// insert one row, returns the self-incrementing id of the inserted data
	id := Table(&user).
		Print().
		Add(&User{
			Nick: "123468900987654321",
		})
	fmt.Println(id)

	ups := Table(&user).
		Print().
		WhereEqual("id", id).
		Mod(UserAvatar, "mod-avatar").
		Ups()
	fmt.Println(ups)

	ups = Table(&user).
		Print().
		WhereEqual("id", id).
		Mod(UserAvatar, "mod-avatar").
		Mod(UserEmail, "mod-email").
		Ups()
	fmt.Println(ups)

	ups = Table(&user).
		Print().
		WhereEqual("id", id).
		Mod(UserAvatar, "mod-avatar").
		Mod(UserEmail, "mod-email").
		Ups(map[string]interface{}{UserMobile: "ups-mobile"})
	fmt.Println(ups)

	// use more map to update
	ups = Table(&user).
		Print().
		WhereEqual("id", id).
		Ups(
			map[string]interface{}{UserMobile: "ups-mobile", UserAvatar: "ups-avatar"},
			map[string]interface{}{UserEmail: "ups-email", UserAvatar: "ups-avatar", UserTime: times})
	fmt.Println(ups)

	dels := Table(&user).
		Print().
		WhereEqual(UserId, id).
		Del()
	fmt.Println(dels)

	dels = Table(&user).
		Print().
		WhereIn(UserStatus, 90, 91).
		WhereOrBracketsLeft().
		WhereOrEqual(UserId, id-1).
		WhereOrEqual(UserId, id).
		WhereBracketsRight().
		Del()
	fmt.Println(dels)

	dels = Table(&user).
		Print().
		WhereLessThanEqual(UserId, 0).
		Del()
	fmt.Println(dels)

	fmt.Println(Table(&user).
		Print().
		Adds(&User{
			Introduce: "1",
		}, &User{
			Introduce: "2",
		}, &User{
			Introduce: "3",
		}))
	fmt.Println(Table(&user).Print().Del())

	tu := Table(&user).Print()
	tu.WhereEqual(UserId, 1).Get(&user)
	fmt.Println(user)

	fmt.Println(tu.WhereEqual(UserId, -200).Del())

	// checkout error
	if tu.Error() != nil {
		fmt.Println(tu.Error())
	}

	// transaction
	begin := Begin().Table(&user).Print()
	id = begin.Add(&User{
		Name: "transaction",
		Time: times,
	})
	if id == 0 {
		fmt.Println("transaction insert error")
		begin.RollBack()
		return
	}
	fmt.Println(id)

	wid := begin.Add(&User{
		Email: "231231231@gmail.com",
		Time:  times,
	})
	fmt.Println(wid)
	if wid == 0 {
		fmt.Println("transaction insert error")
		begin.RollBack()
		return
	}

	if begin.WhereEqual(UserId, id).Mod(UserAvatar, "avatar").Ups() == 0 { // no need to specify the table name again, if your table name doesn't change
		fmt.Println("transaction update error")
		begin.RollBack()
		return
	}
	ups = begin.Table(&User{}).WhereEqual(UserId, wid).Mod(UserNick, "ycm").Ups()
	if ups == 0 {
		fmt.Println("transaction update error")
		begin.RollBack()
		return
	}

	dels = begin.WhereEqual(UserId, wid).Del()
	if dels == 0 {
		fmt.Println("transaction delete error")
		begin.RollBack()
		return
	}

	begin.Commit()
	fmt.Println("successful transaction")

	// checkout error
	fmt.Println(begin.Error())

	// select more rows with group and page data
	users := []*User{}
	err := Table(&user).
		Print().
		Cols(UserId, UserAvatar, UserNick).
		WhereMoreThanEqual(UserId, 0).
		Group(UserId).
		Limit(10).
		Page(5).
		Print().
		Get(&users)
	if err != nil {
		fmt.Println(err)
	}

	for _, v := range users {
		fmt.Println(v.Id, v.Name, v.Avatar, v.Nick)
	}

}
