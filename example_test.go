package go_etcd_clientv3_test
import (
	log "github.com/weizhouBlue/go_log"
	"os"
	"testing"
)



func Test_basic(t *testing.T){
	//c := etcd.Init( []string {"http://127.0.0.1:2379" } , "" , "", "" )


	/*
	c := etcd.Client {
		Tls_cert : "./dce-etcd-cert/etcd-cert" ,
		Tls_ca	: "./dce-etcd-cert/etcd-ca" ,
		Tls_key : "./dce-etcd-cert/etcd-key" ,
	}
	*/

	var c Client

	if ! c.Connect( []string {"http://127.0.0.1:2379" } ) {
		t.Log(  "failed to connect to etcd server" )
		t.FailNow()
	}
	t.Log( "succeeded to connect to etcd server" )
	defer c.Close()

	if ! c.Put("aa" , "100") {
		t.Log("failed to put etcd")
		t.FailNow()		
	}

	t.Log("succeeded to put etcd")

}


