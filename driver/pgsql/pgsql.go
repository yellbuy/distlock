package pgsql

import (
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/lib/pq"
	"github.com/yellbuy/distlock/driver"
)

var (
	MaxReaders            = 1 << 30
	MinWatchRetryInterval = time.Millisecond
	MaxWatchRetryInterval = time.Second * 16
)

type pgsqlDriver struct {
	dsn    []string
	dbs    []*sql.DB
	quorum int
}

var _ driver.IWatcher = &pgsqlDriver{}
var _ driver.IDriver = &pgsqlDriver{}
var _ driver.IRWDriver = &pgsqlDriver{}

func New(dsn ...string) *pgsqlDriver {
	dbs := make([]*sql.DB, len(dsn))
	for i, d := range dsn {
		if db, err := sql.Open("postgres", d); err != nil {
			// normally, it won't happen
			fmt.Println(err)
		} else {
			dbs[i] = db
		}
	}
	return &pgsqlDriver{
		dsn:    dsn,
		dbs:    dbs,
		quorum: len(dbs),
	}
}

func (pd *pgsqlDriver) channelName(name string) string {
	return "unlock-notify-channel-{" + name + "}"
}

func (pd *pgsqlDriver) doLock(fn func(db *sql.DB) int) (bool, time.Duration) {
	counter := pd.quorum
	for _, db := range pd.dbs {
		for {
			if wait := fn(db); wait == 0 {
				time.Sleep(time.Duration(rand.Int31n(1000 * 1000)))
				continue
			} else if wait == -3 {
				counter -= 1
				if counter == 0 {
					return true, 0
				}
			} else if wait > 0 {
				return false, time.Duration(wait) * time.Second
			}
			break
		}
	}
	return false, -1
}

func (pd *pgsqlDriver) doTouch(fn func(db *sql.DB) bool) bool {
	counter := pd.quorum
	for _, db := range pd.dbs {
		if fn(db) {
			counter -= 1
			if counter == 0 {
				return true
			}
		}
	}
	return false
}

func (pd *pgsqlDriver) Lock(name, value string, expiry time.Duration) (ok bool, wait time.Duration) {
	msExpiry := int(expiry / time.Millisecond)
	return pd.doLock(func(db *sql.DB) (wait int) {
		err := db.QueryRow("SELECT common_distlock.lock($1, $2, $3);", name, value, msExpiry).Scan(&wait)
		if err != nil {
			wait = -1 // less than zero, use the default wait duration
			fmt.Println("Lock err", name, err)
		}
		return
	})
}

func (pd *pgsqlDriver) Unlock(name, value string) {
	channel := pd.channelName(name)
	for _, db := range pd.dbs {
		var res bool
		err := db.QueryRow("SELECT common_distlock.unlock($1, $2, $3);", name, value, channel).Scan(&res)
		if err != nil {
			fmt.Println("Unlock err", name, err)
		}
	}
}

func (pd *pgsqlDriver) Touch(name, value string, expiry time.Duration) (ok bool) {
	msExpiry := int(expiry / time.Millisecond)
	return pd.doTouch(func(db *sql.DB) (ok bool) {
		err := db.QueryRow("SELECT common_distlock.touch($1, $2, $3);", name, value, msExpiry).Scan(&ok)
		if err != nil {
			fmt.Println("Touch err", name, err)
		}
		return
	})
}

func (pd *pgsqlDriver) RLock(name, value string, expiry time.Duration) (ok bool, wait time.Duration) {
	msExpiry := int(expiry / time.Millisecond)
	return pd.doLock(func(db *sql.DB) (wait int) {
		err := db.QueryRow("SELECT common_distlock.rlock($1, $2, $3);", name, value, msExpiry).Scan(&wait)
		if err != nil {
			wait = -1 // less than zero, use the default wait duration
			fmt.Println("RLock err", name, err)
		}
		return
	})
}

func (pd *pgsqlDriver) RUnlock(name, value string) {
	channel := pd.channelName(name)
	for _, db := range pd.dbs {
		var res bool
		err := db.QueryRow("SELECT common_distlock.runlock($1, $2, $3, $4);", name, value, channel, MaxReaders).Scan(&res)
		if err != nil {
			fmt.Println("RUnlock err", name, err)
		}
	}
}

func (pd *pgsqlDriver) RTouch(name, value string, expiry time.Duration) (ok bool) {
	msExpiry := int(expiry / time.Millisecond)
	return pd.doTouch(func(db *sql.DB) (ok bool) {
		err := db.QueryRow("SELECT common_distlock.rwtouch($1, $2, $3);", name, value, msExpiry).Scan(&ok)
		if err != nil {
			fmt.Println("RTouch err", name, err)
		}
		return
	})
}

func (pd *pgsqlDriver) WLock(name, value string, expiry time.Duration) (ok bool, wait time.Duration) {
	msExpiry := int(expiry / time.Millisecond)
	return pd.doLock(func(db *sql.DB) (wait int) {
		err := db.QueryRow("SELECT common_distlock.wlock($1, $2, $3, $4);", name, value, msExpiry, MaxReaders).Scan(&wait)
		if err != nil {
			wait = -1 // less than zero, use the default wait duration
			fmt.Println("WLock err", name, err)
		}
		return
	})
}

func (pd *pgsqlDriver) WUnlock(name, value string) {
	channel := pd.channelName(name)
	for _, db := range pd.dbs {
		var res bool
		err := db.QueryRow("SELECT common_distlock.wunlock($1, $2, $3, $4);", name, value, channel, MaxReaders).Scan(&res)
		if err != nil {
			fmt.Println("WUnlock err", name, err)
		}
	}
}

func (pd *pgsqlDriver) WTouch(name, value string, expiry time.Duration) (ok bool) {
	return pd.RTouch(name, value, expiry)
}

func (pd *pgsqlDriver) Watch(name string) <-chan struct{} {
	channel := pd.channelName(name)
	outChan := make(chan struct{})
	for _, dsn := range pd.dsn {
		go func(dsn string) {
			listener := pq.NewListener(dsn, MinWatchRetryInterval, MaxWatchRetryInterval, func(event pq.ListenerEventType, err error) {
				if err != nil {
					fmt.Println("Watch err", channel, err)
				}
			})
			if err := listener.Listen(channel); err != nil {
				// normally, it won't happen
				fmt.Println("Watch err", channel, err)
			}
			for {
				<-listener.Notify
				outChan <- struct{}{}
			}
		}(dsn)
	}
	return outChan
}
