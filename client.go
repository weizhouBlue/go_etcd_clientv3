package etcd_clientv3
import (
    "go.etcd.io/etcd/clientv3"
    "go.etcd.io/etcd/pkg/transport"
    "time"
    "log"
    "os"
    "fmt"
    "context"
    "go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
    "crypto/tls"
    "strings"
)



var (
    dialTimeout    = 5 * time.Second
    requestTimeout = 10 * time.Second
)


type Client struct{
	Tls_cert string
	Tls_ca	string
	Tls_key string
	cli *clientv3.Client
}


func existFile( filePath string ) bool {
	   if info , err := os.Stat(filePath) ; err==nil {
		if ! info.IsDir() {
			return true
		}
    }
    return false
}


func (c *Client) Connect( endpoints []string  ) bool {

	var tlsConfig *tls.Config

	if len(endpoints) ==0 {
		log.Print("error, endpoints ")
		return false
	}

	//if len( c.Tls_ca)!=0 || len( c.Tls_key)!=0 || len( c.Tls_cert)!=0 {
	if strings.Contains( endpoints[0] , "https" ) {
		if existFile( c.Tls_ca)==false   {
			log.Print("error, no file cert-ca ")
			return false
		}
		if existFile( c.Tls_key)==false   {
			log.Print("error, no file cert-key ")
			return false
		}
		if existFile( c.Tls_cert)==false   {
			log.Print("error, no file cert-cert ")
			return false
		}

	    tlsInfo := transport.TLSInfo{
	        CertFile:       c.Tls_cert ,
	        KeyFile:        c.Tls_key ,
	        TrustedCAFile:  c.Tls_ca ,
	    }
	    info, err := tlsInfo.ClientConfig()
	    if err != nil {
	        log.Print(err)
	        return false
	    }
	    tlsConfig=info
	}

    cli, err := clientv3.New(clientv3.Config{
        Endpoints:   endpoints ,
        DialTimeout: 5*time.Second ,
        TLS:         tlsConfig ,
    })
    if err != nil {
        log.Print(err)
        return false
    }
	c.cli=cli
    
    fmt.Println("succeeded to create an etcd client")
    return true
}



func (c *Client) Put( key string , value string  ) bool {
    ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
    _, err := c.cli.Put( ctx , key , value )
    cancel()
    if err != nil {
        switch err {
	        case context.Canceled:
	            fmt.Printf("ctx is canceled by another routine: %v\n", err)
	        case context.DeadlineExceeded:
	            fmt.Printf("ctx is attached with a deadline is exceeded: %v\n", err)
	        case rpctypes.ErrEmptyKey:
	            fmt.Printf("client-side error: %v\n", err)
	        default:
	            fmt.Printf("bad cluster endpoints, which are not etcd servers: %v\n", err)
        }
        return false
    }
    fmt.Printf("succeeded to put , %s=%s \n" , key , value)    
    return true
}




func (c *Client) Close() {
	if c.cli != nil {
		c.cli.Close()
	}
}



