package godatabaseversioner

import (
	"database/sql"
	"fmt"
	"github.com/normegil/postgres"
	"strings"
)

// PostgresVersionApplier is a version applier that will work with a postgres database.
type PostgresVersionApplier struct {
	DB *sql.DB
}

func (a PostgresVersionApplier) CurrentVersion() (int, error) {
	row := a.DB.QueryRow("SELECT version FROM version ORDER BY modificationTime DESC LIMIT 1")
	var version int
	if err := row.Scan(&version); err != nil {
		if a.errIsTableNotExist(err) {
			return -1, nil
		}
		return -1, fmt.Errorf("could not get current version: %w", err)
	}
	return version, nil
}

func (a PostgresVersionApplier) SyncVersion(versionNb int) error {
	if _, err := a.DB.Exec(`INSERT INTO 
    		version(id, version, modificationTime)
    		VALUES (gen_random_uuid(), $1, now())`, versionNb); nil != err {
		return fmt.Errorf("could not insert version %d: %w", versionNb, err)
	}
	return nil
}

func (d PostgresVersionApplier) errIsTableNotExist(err error) bool {
	return strings.Contains(err.Error(), "not exist")
}

// PostgresVersioning is a schema version to install. It will create a versioning table which will hold version number and modification time for each version change
type PostgresVersioning struct {
	DB *sql.DB
	// VersionNumber is strongly suggested to be 0, to be the first modification to do to your database
	VersionNumber int
}

func (v PostgresVersioning) Number() int {
	return v.VersionNumber
}

func (v PostgresVersioning) Upgrade() error {
	row := v.DB.QueryRow(`SELECT pg_catalog.pg_get_userbyid(d.datdba) as "Owner" FROM pg_catalog.pg_database d WHERE d.datname = current_database();`)
	var owner string
	if err := row.Scan(&owner); nil != err {
		return fmt.Errorf("load database owner: %w", err)
	}

	tableExistence := `SELECT EXISTS ( SELECT 1 FROM information_schema.tables WHERE table_name = '%s');`
	tableSetOwner := `ALTER TABLE %s OWNER TO $1;`

	versionTableName := "version"
	err := postgres.CreateTable(v.DB, postgres.TableInfos{
		Queries: map[string]string{
			"Table-Existence": fmt.Sprintf(tableExistence, versionTableName),
			"Table-Create":    `CREATE TABLE version (id uuid primary key, version integer, modificationTime timestamp)`,
			"Table-Set-Owner": fmt.Sprintf(tableSetOwner, versionTableName),
		},
		Owner: owner,
	})
	if err != nil {
		return fmt.Errorf("creating table '%s': %w", versionTableName, err)
	}

	return nil
}

func (v PostgresVersioning) Rollback() error {
	return fmt.Errorf("cannot rollback this change")
}
