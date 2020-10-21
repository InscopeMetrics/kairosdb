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

import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.datapoints.LongDataPointFactory;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class MonitorFilter implements Filter
{
	private final String hostname;
	private final ConcurrentMap<String, AtomicInteger> counterMap = new ConcurrentHashMap<>();
	private final LongDataPointFactory m_dataPointFactory;

	@Inject
	public MonitorFilter(
			final @Named("HOSTNAME")String hostname,
			final LongDataPointFactory dataPointFactory,
			final PeriodicMetrics periodicMetrics)
	{
		this.hostname = checkNotNullOrEmpty(hostname);
		m_dataPointFactory = dataPointFactory;

		periodicMetrics.registerPolledMetric(m -> {
			for (final String resource : counterMap.keySet()) {
				m.recordGauge(
					"rest_service/" + resource,
						counterMap.get(resource).getAndSet(0));
			}
		});
	}

	@Override
	public void init(FilterConfig filterConfig)
	{
	}

	@Override
	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException
	{
		String path = ((HttpServletRequest)servletRequest).getRequestURI();
		String resourceName = ((HttpServletRequest) servletRequest).getMethod() + path;

		if (resourceName.length() > 0)
		{
			AtomicInteger counter = counterMap.get(resourceName);
			if (counter == null)
			{
				counter = new AtomicInteger();
				AtomicInteger mapValue = counterMap.putIfAbsent(resourceName, counter);
				counter = (mapValue != null ? mapValue : counter);
			}
			counter.incrementAndGet();
		}

		filterChain.doFilter(servletRequest, servletResponse);
	}

	@Override
	public void destroy()
	{
	}
}
