package sql

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/abiosoft/ishell/v2"
	"github.com/chrusty/go-tableprinter"
	"github.com/spf13/cobra"
	"github.com/suryatresna/devkit/pkg/database"
)

// sqlShellCmd represents the produce command
var sqlShellCmd = &cobra.Command{
	Use:   "query",
	Short: "query database",
	Long: `Query database via command
	Example:
		app sql query --type mysql --host 
	`,
	Run: sqlShellHandler,
}

func init() {
	SqlCmd.AddCommand(sqlShellCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// sqlShellCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// sqlShellCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	sqlShellCmd.Flags().StringP("type", "t", "", "type of database (mysql, postgres, sqlite)")
	sqlShellCmd.Flags().StringP("host", "s", "", "host of database")
	sqlShellCmd.Flags().StringP("port", "p", "", "port of database")
	sqlShellCmd.Flags().StringP("user", "u", "", "user of database")
	sqlShellCmd.Flags().StringP("password", "P", "", "password of database")
	sqlShellCmd.Flags().StringP("database", "d", "", "database name")
	sqlShellCmd.Flags().StringP("sslmode", "l", "", "sslmode")

}

func sqlShellHandler(cmd *cobra.Command, args []string) {
	sqlcon, err := database.NewConnection(database.Config{
		Type:     cmd.Flag("type").Value.String(),
		Address:  cmd.Flag("host").Value.String(),
		Port:     cmd.Flag("port").Value.String(),
		Username: cmd.Flag("user").Value.String(),
		Password: cmd.Flag("password").Value.String(),
		DBName:   cmd.Flag("database").Value.String(),
		SslMode:  cmd.Flag("sslmode").Value.String(),
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Connected to database")

	sqlSh := newSqlShell(sqlcon)

	shell := ishell.New()

	shell.AddCmd(&ishell.Cmd{
		Name: "sh",
		Help: "shell mode",
		Func: sqlSh.SqlShellContext,
	})

	shell.Run()
}

type sqlShell struct {
	db database.DBInterface
}

func newSqlShell(db database.DBInterface) sqlShell {
	return sqlShell{
		db: db,
	}
}

func (s sqlShell) SqlShellContext(c *ishell.Context) {
	c.Println("Sql shell mode. Type `;` to endlines")
	qry := c.ReadMultiLines(";")
	rows, err := s.db.Queryx(qry)
	if err != nil {
		c.Println(err)
		return
	}
	defer rows.Close()

	var out []map[string]interface{} = []map[string]interface{}{}

	for rows.Next() {
		// Create a slice of interface{}'s to represent each column,
		// and a second slice to contain pointers to each item in the columns slice.
		var outRow map[string]interface{} = map[string]interface{}{}
		if errMap := rows.MapScan(outRow); errMap != nil {
			c.Println(errMap)
			return
		}

		var newOutRow map[string]interface{} = map[string]interface{}{}
		for k, v := range outRow {
			switch v.(type) {
			case []uint8:
				newOutRow[k] = fmt.Sprintf("%.2f", parseFloat(v.([]uint8)))
			case int64:
				newOutRow[k] = strconv.FormatInt(v.(int64), 10)
			case time.Time:
				newOutRow[k] = v.(time.Time).Format("2006-01-02 15:04:05")
			case nil:
				newOutRow[k] = "NULL"
			default:
				fmt.Printf("col:%s ;Type: %v\n", k, reflect.TypeOf(v))
				newOutRow[k] = v
			}
		}

		out = append(out, newOutRow)
	}

	c.Println("Result:\n")
	tableprinter.SetSortedHeaders(true)
	tableprinter.Print(out)
}

func parseFloat(bytes []uint8) float64 {
	f, err := strconv.ParseFloat(string(bytes), 64)
	if err != nil {
		return math.NaN()
	}
	return f
}
