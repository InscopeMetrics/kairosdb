/*
 * Copyright 2016 KairosDB Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.kairosdb.core.http;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.google.inject.servlet.GuiceFilter;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.security.Credential;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.exception.KairosDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;


public class WebServer implements KairosDBService {
    public static final Logger logger = LoggerFactory.getLogger(WebServer.class);

    public static final String JETTY_ADDRESS_PROPERTY = "kairosdb.jetty.address";
    public static final String JETTY_PORT_PROPERTY = "kairosdb.jetty.port";
    public static final String JETTY_WEB_ROOT_PROPERTY = "kairosdb.jetty.static_web_root";
    public static final String JETTY_AUTH_USER_PROPERTY = "kairosdb.jetty.basic_auth.user";
    public static final String JETTY_AUTH_PASSWORD_PROPERTY = "kairosdb.jetty.basic_auth.password";
    public static final String JETTY_SSL_PORT = "kairosdb.jetty.ssl.port";
    public static final String JETTY_SSL_PROTOCOLS = "kairosdb.jetty.ssl.protocols";
    public static final String JETTY_SSL_CIPHER_SUITES = "kairosdb.jetty.ssl.cipherSuites";
    public static final String JETTY_SSL_KEYSTORE_PATH = "kairosdb.jetty.ssl.keystore.path";
    public static final String JETTY_SSL_KEYSTORE_PASSWORD = "kairosdb.jetty.ssl.keystore.password";
    public static final String JETTY_THREADS_QUEUE_SIZE_PROPERTY = "kairosdb.jetty.threads.queue_size";
    public static final String JETTY_THREADS_MIN_PROPERTY = "kairosdb.jetty.threads.min";
    public static final String JETTY_THREADS_MAX_PROPERTY = "kairosdb.jetty.threads.max";
    public static final String JETTY_THREADS_KEEP_ALIVE_MS_PROPERTY = "kairosdb.jetty.threads.keep_alive_ms";
    public static final String JETTY_SHOW_STACKTRACE = "kairosdb.jetty.show_stacktrace";

    private final InetAddress m_address;
    private final int m_port;
    private final String m_webRoot;
    private Server m_server;
    private String m_authUser = null;
    private String m_authPassword = null;
    private int m_sslPort;
    private String[] m_cipherSuites;
    private String[] m_protocols;
    private String m_keyStorePath;
    private String m_keyStorePassword;
    private ExecutorThreadPool m_pool;
    private boolean m_showStacktrace;

    public WebServer(final int port, final String webRoot)
            throws UnknownHostException {
        this(null, port, webRoot);
    }

    @Inject
    public WebServer(
            @Named(JETTY_ADDRESS_PROPERTY) final String address,
            @Named(JETTY_PORT_PROPERTY) final int port,
            @Named(JETTY_WEB_ROOT_PROPERTY) final String webRoot)
            throws UnknownHostException {
        checkNotNull(webRoot);

        m_port = port;
        m_webRoot = webRoot;
        m_address = InetAddress.getByName(address);
    }

    private static SecurityHandler basicAuth(final String username, final String password, final String realm) {

        final HashLoginService l = new HashLoginService();
        l.putUser(username, Credential.getCredential(password), new String[]{"user"});
        l.setName(realm);

        final Constraint constraint = new Constraint();
        constraint.setName(Constraint.__BASIC_AUTH);
        constraint.setRoles(new String[]{"user"});
        constraint.setAuthenticate(true);

        final Constraint noConstraint = new Constraint();

        final ConstraintMapping healthcheckConstraintMapping = new ConstraintMapping();
        healthcheckConstraintMapping.setConstraint(noConstraint);
        healthcheckConstraintMapping.setPathSpec("/api/v1/health/check");

        final ConstraintMapping cm = new ConstraintMapping();
        cm.setConstraint(constraint);
        cm.setPathSpec("/*");

        final ConstraintSecurityHandler csh = new ConstraintSecurityHandler();
        csh.setAuthenticator(new BasicAuthenticator());
        csh.setRealmName("myrealm");
        csh.addConstraintMapping(healthcheckConstraintMapping);
        csh.addConstraintMapping(cm);
        csh.setLoginService(l);

        return csh;

    }

    @Inject(optional = true)
    public void setAuthCredentials(@Named(JETTY_AUTH_USER_PROPERTY) final String user,
                                   @Named(JETTY_AUTH_PASSWORD_PROPERTY) final String password) {
        m_authUser = user;
        m_authPassword = password;
    }

    @Inject(optional = true)
    public void setSSLSettings(@Named(JETTY_SSL_PORT) final int sslPort,
                               @Named(JETTY_SSL_KEYSTORE_PATH) final String keyStorePath,
                               @Named(JETTY_SSL_KEYSTORE_PASSWORD) final String keyStorePassword) {
        m_sslPort = sslPort;
        m_keyStorePath = checkNotNullOrEmpty(keyStorePath);
        m_keyStorePassword = checkNotNullOrEmpty(keyStorePassword);
    }

    @Inject(optional = true)
    public void setSSLCipherSuites(@Named(JETTY_SSL_CIPHER_SUITES) final String cipherSuites) {
        checkNotNull(cipherSuites);
        m_cipherSuites = cipherSuites.split("\\s*,\\s*");
    }

    @Inject(optional = true)
    public void setSSLProtocols(@Named(JETTY_SSL_PROTOCOLS) final String protocols) {
        m_protocols = protocols.split("\\s*,\\s*");
    }

    @Inject(optional = true)
    public void setThreadPool(@Named(JETTY_THREADS_QUEUE_SIZE_PROPERTY) final int maxQueueSize,
                              @Named(JETTY_THREADS_MIN_PROPERTY) final int minThreads,
                              @Named(JETTY_THREADS_MAX_PROPERTY) final int maxThreads,
                              @Named(JETTY_THREADS_KEEP_ALIVE_MS_PROPERTY) final long keepAliveMs) {
        final LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(maxQueueSize);
        m_pool = new ExecutorThreadPool(minThreads, maxThreads, keepAliveMs, TimeUnit.MILLISECONDS, queue);
    }

    @Inject
    public void setJettyShowStacktrace(@Named(JETTY_SHOW_STACKTRACE) boolean showStacktrace) {
        m_showStacktrace = showStacktrace;
    }

    @Override
    public void start() throws KairosDBException {
        try {
            if (m_port > 0)
                m_server = new Server(new InetSocketAddress(m_address, m_port));
            else
                m_server = new Server();

            if (m_pool != null)
                m_server.setThreadPool(m_pool);

            //Error handler
            ErrorHandler errorHandler = new ErrorHandler();
            errorHandler.setShowStacks(m_showStacktrace);
            m_server.addBean(errorHandler);

            //Set up SSL
            if (m_keyStorePath != null && !m_keyStorePath.isEmpty()) {
                logger.info("Using SSL");
                final SslContextFactory sslContextFactory = new SslContextFactory(m_keyStorePath);

                if (m_cipherSuites != null && m_cipherSuites.length > 0)
                    sslContextFactory.setIncludeCipherSuites(m_cipherSuites);

                if (m_protocols != null && m_protocols.length > 0)
                    sslContextFactory.setIncludeProtocols(m_protocols);

                sslContextFactory.setKeyStorePassword(m_keyStorePassword);
                final SslSelectChannelConnector selectChannelConnector = new SslSelectChannelConnector(sslContextFactory);
                selectChannelConnector.setPort(m_sslPort);
                m_server.addConnector(selectChannelConnector);
            }

            final ServletContextHandler servletContextHandler =
                    new ServletContextHandler();

            //Turn on basic auth if the user was specified
            if (m_authUser != null) {
                servletContextHandler.setSecurityHandler(basicAuth(m_authUser, m_authPassword, "kairos"));
                servletContextHandler.setContextPath("/");
            }

            servletContextHandler.addFilter(GuiceFilter.class, "/api/*", null);
            servletContextHandler.addServlet(DefaultServlet.class, "/api/*");

            final ResourceHandler resourceHandler = new ResourceHandler();
            resourceHandler.setDirectoriesListed(true);
            resourceHandler.setWelcomeFiles(new String[]{"index.html"});
            resourceHandler.setResourceBase(m_webRoot);
            resourceHandler.setAliases(true);

            final HandlerList handlers = new HandlerList();
            handlers.setHandlers(new Handler[]{servletContextHandler, resourceHandler, new DefaultHandler()});
            m_server.setHandler(handlers);

            m_server.start();
        } catch (final Exception e) {
            throw new KairosDBException(e);
        }
    }

    @Override
    public void stop() {
        try {
            if (m_server != null) {
                m_server.stop();
                m_server.join();
            }
        } catch (final Exception e) {
            logger.error("Error stopping web server", e);
        }
    }

    public InetAddress getAddress() {
        return m_address;
    }
}
