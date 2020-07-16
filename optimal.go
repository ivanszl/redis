package redis

import (
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"sync"
	"time"
	"sort"

	"github.com/go-redis/redis/internal"
	"github.com/go-redis/redis/internal/pool"
)

func defaultPerfectSlave(slaves map[string]string) string {
	var keys []string
	for k, _ := range slaves {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return slaves[keys[0]]
}

//------------------------------------------------------------------------------

// OptimalOptions are used to configure a Optimal Sentinel client and should
// be passed to NewOptimalClient.
type OptimalOptions struct {
	// The master name.
	MasterName string
	// A seed list of host:port addresses of sentinel nodes.
	SentinelAddrs []string

	// Following options are copied from Options struct.

	OnConnect func(*Conn) error

	PerfectSlave func(map[string]string) string

	Password string
	DB       int

	MaxRetries      int
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	PoolSize           int
	MinIdleConns       int
	MaxConnAge         time.Duration
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration

	TLSConfig *tls.Config
}

func (opt *OptimalOptions) options() *Options {
	return &Options{
		Addr: "FailoverClient",

		OnConnect: opt.OnConnect,

		DB:       opt.DB,
		Password: opt.Password,

		MaxRetries: opt.MaxRetries,

		DialTimeout:  opt.DialTimeout,
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,

		PoolSize:           opt.PoolSize,
		PoolTimeout:        opt.PoolTimeout,
		IdleTimeout:        opt.IdleTimeout,
		IdleCheckFrequency: opt.IdleCheckFrequency,

		TLSConfig: opt.TLSConfig,
	}
}

func (c *sentinelFailover) getSlaves() map[string]string {
	defer func() {
		if err := recover(); err != nil {
			internal.Logf("sentinel: Slaves master=%q failed: %q",
			c.masterName, err)
		}
		return
	}()
	sentinel := c.sentinel

	if sentinel == nil {
		return nil
	}

	ret, err := sentinel.Slaves().Result()
	if err != nil {
		internal.Logf("sentinel: Slaves master=%q failed: %s",
			c.masterName, err)
		return nil
	}
	result := map[string]string{}
	for _, slave := range ret {
		if v, ok := slave.(map[string]interface{}); ok {
			if v["master-link-status"].(string) == "ok" {
				result[v["name"].(string)] = net.JoinHostPort(v["ip"].(string), v["port"].(string))
			}
		}
	}
	return result
}



func (c *sentinelFailover) slaves() (map[string]string, error) {
	c.mu.RLock()
	val := c.getSlaves()
	c.mu.RUnlock()
	if val != nil && len(val) != 0 {
		return val, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	val = c.getSlaves()
	if val != nil && len(val) != 0 {
		return val, nil
	}

	if c.sentinel != nil {
		c.closeSentinel()
	}

	val = map[string]string{}

	err := c.selectSentinel(func(sentinel *SentinelClient) error {
		defer func() {
			if err := recover(); err != nil {
				internal.Logf("sentinel: Slaves master=%q failed: %q",
				c.masterName, err)
			}
			return
		}()
		ret, err := sentinel.Slaves().Result()
		if err != nil {
			internal.Logf("sentinel: Slaves master=%q failed: %s",
				c.masterName, err)
			return err
		}
		for _, slave := range ret {
			if v, ok := slave.(map[string]interface{}); ok {
				if v["master-link-status"].(string) == "ok" {
					val[v["name"].(string)] = net.JoinHostPort(v["ip"].(string), v["port"].(string))
				}
			}
		}
		if len(val) == 0 {
			return errors.New("redis: not found any slaves")
		}
	})
	if err != nil {
		return nil, err
	}

	return val, nil
}

func (c *sentinelFailover) Slaves() (map[string]string, error) {
	ret, err := c.slaves()
	if err != nil {
		return nil, err
	}
	return ret, nil
}

//------------------------------------------------------------------------------

// OptimalSentinel is a Redis sentinel can select the optimal client representing a pool of zero or more
// underlying connections. It's safe for concurrent use by multiple
// goroutines.
type OptimalSentinel struct {
	masterClient *client
	slaveClient *client
}

// NewOptimalSentinel returns a Redis Sentinel can select the optimal client
// that uses Redis Sentinel for automatic failover.
// It's safe for concurrent use by multiple
// goroutines.
func NewOptimalSentinel(optimalOpt *OptimalOptions) *OptimalSentinel {
	opt := optimalOpt.options()
	opt.init()

	failover := &sentinelFailover{
		masterName:    optimalOpt.MasterName,
		sentinelAddrs: optimalOpt.SentinelAddrs,
		opt: opt,
	}

	if optimalOpt.PerfectSlave == nil {
		optimalOpt.PerfectSlave = defaultPerfectSlave
	}

	master := Client{
		baseClient: baseClient{
			opt:      opt,
			connPool: failover.Pool(),

			onClose: func() error {
				return failover.Close()
			},
		},
	}
	master.baseClient.init()
	master.cmdable.setProcessor(c.Process)

	slaveFailover := &slaveFailover{
		failover: failover,
		perfectSlave: optimalOpt.PerfectSlave
	}
	
	slave := Client{
		baseClient: baseClient{
			opt:      opt,
			connPool: slaveFailover.Pool(),
		},
	}
	slave.baseClient.init()
	slave.cmdable.setProcessor(c.Process)

	return &OptimalSentinel{
		masterClient: master,
		slaveClient: slave,
	}
}

func (s *OptimalSentinel) Master() *Client {
	return s.masterClient
}

func (s *OptimalSentinel) Slave() *Client {
	return s.slaveClient
}

func (s *OptimalSentinel) Close() error {
	var err error
	if s.masterClient != nil {
		err = s.masterClient.Close()
	}
	if s.slaveClient != nil {
		s.slaveClient.Close()
	}
	return err
}

type slaveFailover struct {
	failover *sentinelFailover
	pool     *pool.ConnPool
	poolOnce sync.Once
	mu          sync.RWMutex
	perfectSlave func(map[string]string) string
	_slaveAddr string
}

func (c *slaveFailover) Pool() *pool.ConnPool {
	c.poolOnce.Do(func() {
		c.opt.Dialer = c.dial
		c.pool = newConnPool(c.failover.opt)
	})
	return c.pool
}

func (c *slaveFailover) dial()  (net.Conn, error) {
	slaves, err := c.failover.Slaves()
	var addr string
	if err != nil {
		addr, err = c.failover.MasterAddr()
		if err != nil {
			return nil, err
		}
	} else {
		addr = c.perfectSlave(slaves)
	}
	c.switchSlave(addr)
	return net.DialTimeout("tcp", addr, c.failover.opt.DialTimeout)
}

func (c *slaveFailover) switchSlave(addr string) {
	c.mu.RLock()
	masterAddr := c._slaveAddr
	c.mu.RUnlock()
	if _slaveAddr == addr {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	internal.Logf("sentinel: new master=%q addr=%q",
		c.masterName, addr)
	_ = c.Pool().Filter(func(cn *pool.Conn) bool {
		return cn.RemoteAddr().String() != addr
	})
	c._slaveAddr = addr
}

