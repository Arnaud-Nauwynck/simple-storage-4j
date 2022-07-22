package org.simplestorage4j.opsserver.configuration;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.simplestorage4j.api.util.BlobStorageUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import lombok.Getter;
import lombok.val;

@Configuration
@EnableWebSecurity
@Order(1)
public class ApiKeyAuthSecurityConfiguration extends WebSecurityConfigurerAdapter {

	@Autowired
	private OpsServerAppParams opServerAppParams;

	@Override
    protected void configure(HttpSecurity httpSecurity) throws Exception {
		val authHeaderName = opServerAppParams.getServerAuthHeaderName();
		Map<String,ApiKeyAuthenticationToken> apiKeys = new HashMap<>();
		val paramAuthApiKeys = opServerAppParams.getAuthApiKeys();
		for(val paramAuthApiKey: paramAuthApiKeys) {
			val apiKey = paramAuthApiKey.getApiKey();
			val principal = paramAuthApiKey.getPrincipal();
			val authorities = BlobStorageUtils.map(paramAuthApiKey.getAuthorities(), x -> new SimpleGrantedAuthority(x));
			val auth = new ApiKeyAuthenticationToken(principal, apiKey, authorities);
			apiKeys.put(apiKey, auth);
		}
		
		ApiKeyAuthenticationFilter apiKeyAuthFilter = new ApiKeyAuthenticationFilter(authHeaderName, apiKeys);
        httpSecurity.addFilterBefore(apiKeyAuthFilter, UsernamePasswordAuthenticationFilter.class);
    
        // TOCHECK
        httpSecurity.csrf().disable();
	}

	// ------------------------------------------------------------------------

	/**
	 * spring-security Authentication for api-key
	 */
	public static class ApiKeyAuthenticationToken extends AbstractAuthenticationToken {

		/** */
		private static final long serialVersionUID = 1L;
		
		private final String principal;
		@Getter
		private final String apiKey;
	    
	    public ApiKeyAuthenticationToken(String principal, String apiKey, Collection<? extends GrantedAuthority> authorities) {
	        super(authorities);
	        this.principal = principal;
	        this.apiKey = apiKey;
	        setAuthenticated(true);
	    }

	    @Override
	    public Object getCredentials() {
	        return null; // apiKey
	    }

	    @Override
	    public Object getPrincipal() {
	        return principal;
	    }

	}

	// ------------------------------------------------------------------------

	/**
	 * javax.servlet.Filter for authenticating with api-key http header
	 */
	public static class ApiKeyAuthenticationFilter implements Filter {
	
	    private final @Nonnull String apiKeyHeaderName; // "api-key"
		private final @Nonnull Map<String,ApiKeyAuthenticationToken> apiKeys;
		
	    public ApiKeyAuthenticationFilter(@Nonnull String apiKeyHeaderName, @Nonnull Map<String, ApiKeyAuthenticationToken> apiKeys) {
			this.apiKeyHeaderName = apiKeyHeaderName;
	    	this.apiKeys = apiKeys;
		}

		@Override
	    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
	            throws IOException, ServletException {
	        if (request instanceof HttpServletRequest && response instanceof HttpServletResponse) {
	        	val httpRequest = (HttpServletRequest) request;
	        	val authApiKey = httpRequest.getHeader(apiKeyHeaderName);
	            if (authApiKey != null) {
	            	val found = apiKeys.get(authApiKey);
	                if (found != null) {
	                    SecurityContextHolder.getContext().setAuthentication(found);
	                } else {
	                    HttpServletResponse httpResponse = (HttpServletResponse) response;
	                    httpResponse.setStatus(401);
	                    httpResponse.getWriter().write("Invalid API Key");
	                    return;
	                }
	            }
	        }
	        chain.doFilter(request, response);
	    }
	}
	
}
