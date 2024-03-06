package com.kalsym.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeoutException;
import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.exception.MemcachedException;
import net.rubyeye.xmemcached.utils.AddrUtil;

/**
 *
 * @author Aakash
 */
public class MemcachedHandling {

    private MemcachedClientBuilder builder;
    private final MemcachedClient client;
    final int default_expiry = 3600; //1 hour
    static private int expiry;

    /**
     * used to get Memcache expiray time in seconds
     *
     * @return return expiry if it is greater than 0 or returns default 3600 //1
     * hour
     */
    public int getExpiry() {
        if (expiry <= 0) {
            return default_expiry;
        }
        return expiry;
    }

    /**
     * used to set value of memcache entry expiry in seconds
     *
     * @param expiry in seconds
     */
    public void setExpiry(int expiry) {
        MemcachedHandling.expiry = expiry;
    }

    /**
     * parameterize constructor for one client: initialize memcache client on
     * host ip and port, with verbosity and log level
     *
     * @param hostname
     * @param port
     * @param verbosity
     * @throws IOException
     * @throws java.util.concurrent.TimeoutException
     * @throws java.lang.InterruptedException
     * @throws net.rubyeye.xmemcached.exception.MemcachedException
     */
    public MemcachedHandling(String hostname, int port, int verbosity) throws IOException, TimeoutException, InterruptedException, MemcachedException {
        client = new XMemcachedClient(hostname, port);
        setLogLevel(verbosity);
        //.setOpTimeout(500L);
    }

    /**
     * paramterize constructor for tw0 client: intialzes memcache client on host
     * IPs, ports and weights , with verbosity and log level
     *
     * @param hostname1
     * @param port1
     * @param weight1
     * @param hostname2
     * @param port2
     * @param weight2
     * @param verbosity
     * @throws IOException
     * @throws java.util.concurrent.TimeoutException
     * @throws java.lang.InterruptedException
     * @throws net.rubyeye.xmemcached.exception.MemcachedException
     */
    public MemcachedHandling(String hostname1, int port1, int weight1,
            String hostname2, int port2, int weight2, int verbosity) throws IOException, TimeoutException, InterruptedException, MemcachedException {
        builder = new XMemcachedClientBuilder(AddrUtil.getAddresses(hostname1 + ":" + port1 + " " + hostname2 + ":" + port2), new int[]{weight1, weight2});
        client = builder.build();
        //AddrUtil.getOneAddress("localhost:11211")
        setLogLevel(verbosity);
    }

    /**
     * inserts subscriber type in memcache as an entry
     *
     * @param msisdn
     * @param subtype
     * @throws java.util.concurrent.TimeoutException
     * @throws java.lang.InterruptedException
     * @throws net.rubyeye.xmemcached.exception.MemcachedException
     */
    public void putSubType(String msisdn, String subtype) throws TimeoutException, InterruptedException, MemcachedException {
        try {
//            LogProperties.WriteLog("[Memcached] Putting key:" + msisdn + "_subtype value:" + subtype);

            client.add(msisdn + "_subtype", 86400, subtype);

        } catch (SecurityException ex) {
            throw ex;
//            LogProperties.WriteLog("[Memcached]" + ex);
        } catch (InterruptedException iEx) {
            throw iEx;
//            LogProperties.WriteLog("[Memcached]" + ex);
        } catch (MemcachedException mEx) {
            throw mEx;
//            LogProperties.WriteLog("[Memcached]" + ex);
        }
    }

    /**
     * get subscriber type from memcache if it exists else returns ""
     *
     * @param msisdn
     * @return
     * @throws java.util.concurrent.TimeoutException
     * @throws java.lang.InterruptedException
     * @throws net.rubyeye.xmemcached.exception.MemcachedException
     */
    public String getSubType(String msisdn) throws TimeoutException, InterruptedException, MemcachedException {
//        LogProperties.WriteLog("Getting subtype from cache msisdn:" + msisdn);
        try {
            String subType = client.get(msisdn + "_subtype");
            if (subType == null) {
                return "";
            } else {
                return subType;
            }
        } catch (SecurityException ex) {
            throw ex;
//            LogProperties.WriteLog("[Memcached]" + ex);
        } catch (InterruptedException iEx) {
            throw iEx;
//            LogProperties.WriteLog("[Memcached]" + ex);
        } catch (MemcachedException mEx) {
            throw mEx;
//            LogProperties.WriteLog("[Memcached]" + ex);
        }
//        return "";
    }

    /**
     * puts new entry in memCache
     *
     * @param key
     * @param data
     * @return true if successful, false otherwise
     * @throws java.util.concurrent.TimeoutException
     * @throws java.lang.InterruptedException
     * @throws net.rubyeye.xmemcached.exception.MemcachedException
     */
    public boolean putInMemcached(String key, Object data) throws TimeoutException, InterruptedException, MemcachedException {
        try {
            client.add(key, expiry, data);
        } catch (TimeoutException ex) {
//            LogProperties.WriteLog("[Memcached]" + ex);
            return false;
        } catch (InterruptedException ex) {
//            LogProperties.WriteLog("[Memcached]" + ex);
            return false;
        } catch (MemcachedException ex) {
//            LogProperties.WriteLog("[Memcached]" + ex);
            return false;
        }
        return true;
    }

    /**
     *
     * @param key
     * @return true if success else returns false in case of exception
     * @throws java.util.concurrent.TimeoutException
     * @throws java.lang.InterruptedException
     * @throws net.rubyeye.xmemcached.exception.MemcachedException
     */
    public boolean removeFromMemcached(String key) throws TimeoutException, InterruptedException, MemcachedException {
        try {
//            LogProperties.WriteLog("[Memcached] deleting : [" + key + "]");
            client.delete(key);
//            LogProperties.WriteLog("[Memcached] deleted : [" + key + "]");
            return true;
        } catch (TimeoutException ex) {
//            LogProperties.WriteLog("[Memcached]" + ex);
            return false;
        } catch (InterruptedException ex) {
//            LogProperties.WriteLog("[Memcached]" + ex);
            return false;
        } catch (MemcachedException ex) {
//            LogProperties.WriteLog("[Memcached]" + ex);
            return false;
        }
    }

    /**
     *
     * @param key
     * @return Object if key is in memCache, otherwise null
     * @throws java.util.concurrent.TimeoutException
     * @throws java.lang.InterruptedException
     * @throws net.rubyeye.xmemcached.exception.MemcachedException
     */
    public Object getFromMemcache(String key) throws TimeoutException, InterruptedException, MemcachedException {
        try {
//            LogProperties.WriteLog("[Memcached] Getting Key: " + key);
            Object arr = (String) client.get(key);
//            LogProperties.WriteLog("[Memcached] getNotification return: " + arr);
            return arr;
        } catch (TimeoutException ex) {
//            LogProperties.WriteLog("[Memcached]" + ex);
            return null;
        } catch (InterruptedException ex) {
//            LogProperties.WriteLog("[Memcached]" + ex);
            return null;
        } catch (MemcachedException ex) {
//            LogProperties.WriteLog("[Memcached]" + ex);
            return null;
        }
    }

    /**
     * Sets logging level and verbosity of memCache client
     *
     * @param verbosity
     * @throws java.lang.InterruptedException
     * @throws net.rubyeye.xmemcached.exception.MemcachedException
     */
    public final void setLogLevel(int verbosity) throws SecurityException, InterruptedException, MemcachedException {
        try {
            this.client.setLoggingLevelVerbosityWithNoReply((InetSocketAddress) client.getAvailableServers().toArray()[0], verbosity);
            if (client.getAvailableServers().toArray().length > 1) {
                this.client.setLoggingLevelVerbosityWithNoReply((InetSocketAddress) client.getAvailableServers().toArray()[1], verbosity);
            }
        } catch (SecurityException ex) {
            throw ex;
//            LogProperties.WriteLog("[Memcached]" + ex);
        } catch (InterruptedException iEx) {
            throw iEx;
//            LogProperties.WriteLog("[Memcached]" + ex);
        } catch (MemcachedException mEx) {
            throw mEx;
//            LogProperties.WriteLog("[Memcached]" + ex);
        }
    }
}
