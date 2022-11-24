/*
 * Copyright 2014-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.LogFactory;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ContainerGroup;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.EndpointHandlerMethod;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Creates the necessary {@link MessageListenerContainer} instances for the
 * registered {@linkplain KafkaListenerEndpoint endpoints}. Also manages the
 * lifecycle of the listener containers, in particular within the lifecycle
 * of the application context.
 *
 * <p>Contrary to {@link MessageListenerContainer}s created manually, listener
 * containers managed by registry are not beans in the application context and
 * are not candidates for autowiring. Use {@link #getListenerContainers()} if
 * you need to access this registry's listener containers for management purposes.
 * If you need to access to a specific message listener container, use
 * {@link #getListenerContainer(String)} with the id of the endpoint.
 *
 * @author Stephane Nicoll
 * @author Juergen Hoeller
 * @author Artem Bilan
 * @author Gary Russell
 * @author Asi Bross
 *
 * @see KafkaListenerEndpoint
 * @see MessageListenerContainer
 * @see KafkaListenerContainerFactory
 */
public class KafkaListenerEndpointRegistry implements ListenerContainerRegistry, DisposableBean, SmartLifecycle,
		ApplicationContextAware, ApplicationListener<ContextRefreshedEvent> {

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); //NOSONAR

	private final Map<String, MessageListenerContainer> unregisteredContainers = new ConcurrentHashMap<>();

	private final Map<String, MessageListenerContainer> listenerContainers = new ConcurrentHashMap<>();

	private int phase = AbstractMessageListenerContainer.DEFAULT_PHASE;

	private ConfigurableApplicationContext applicationContext;

	private boolean contextRefreshed;

	private boolean alwaysStartAfterRefresh = true;

	private volatile boolean running;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		if (applicationContext instanceof ConfigurableApplicationContext) {
			this.applicationContext = (ConfigurableApplicationContext) applicationContext;
		}
	}

	/**
	 * Return the {@link MessageListenerContainer} with the specified id or
	 * {@code null} if no such container exists.
	 * @param id the id of the container
	 * @return the container or {@code null} if no container with that id exists
	 * @see KafkaListenerEndpoint#getId()
	 * @see #getListenerContainerIds()
	 */
	@Override
	@Nullable
	public MessageListenerContainer getListenerContainer(String id) {
		Assert.hasText(id, "Container identifier must not be empty");
		return this.listenerContainers.get(id);
	}

	@Override
	@Nullable
	public MessageListenerContainer getUnregisteredListenerContainer(String id) {
		MessageListenerContainer container = this.unregisteredContainers.get(id);
		if (container == null) {
			refreshContextContainers();
			return this.unregisteredContainers.get(id);
		}
		return null;
	}

	/**
	 * By default, containers registered for endpoints after the context is refreshed
	 * are immediately started, regardless of their autoStartup property, to comply with
	 * the {@link SmartLifecycle} contract, where autoStartup is only considered during
	 * context initialization. Set to false to apply the autoStartup property, even for
	 * late endpoint binding. If this is called after the context is refreshed, it will
	 * apply to any endpoints registered after that call.
	 * @param alwaysStartAfterRefresh false to apply the property.
	 * @since 2.8.7
	 */
	public void setAlwaysStartAfterRefresh(boolean alwaysStartAfterRefresh) {
		this.alwaysStartAfterRefresh = alwaysStartAfterRefresh;
	}

	/**
	 * Return the ids of the managed {@link MessageListenerContainer} instance(s).
	 * @return the ids.
	 * @see #getListenerContainer(String)
	 */
	@Override
	public Set<String> getListenerContainerIds() {
		return Collections.unmodifiableSet(this.listenerContainers.keySet());
	}

	/**
	 * Return the managed {@link MessageListenerContainer} instance(s).
	 * @return the managed {@link MessageListenerContainer} instance(s).
	 * @see #getAllListenerContainers()
	 */
	@Override
	public Collection<MessageListenerContainer> getListenerContainers() {
		return Collections.unmodifiableCollection(this.listenerContainers.values());
	}

	/**
	 * Return all {@link MessageListenerContainer} instances including those managed by
	 * this registry and those declared as beans in the application context.
	 * Prototype-scoped containers will be included. Lazy beans that have not yet been
	 * created will not be initialized by a call to this method.
	 * @return the {@link MessageListenerContainer} instance(s).
	 * @since 2.2.5
	 * @see #getListenerContainers()
	 */
	@Override
	public Collection<MessageListenerContainer> getAllListenerContainers() {
		List<MessageListenerContainer> containers = new ArrayList<>();
		containers.addAll(getListenerContainers());
		refreshContextContainers();
		containers.addAll(this.unregisteredContainers.values());
		return containers;
	}

	private void refreshContextContainers() {
		this.unregisteredContainers.clear();
		this.applicationContext.getBeansOfType(MessageListenerContainer.class, true, false).values()
				.forEach(container -> this.unregisteredContainers.put(container.getListenerId(), container));
	}

	/**
	 * Create a message listener container for the given {@link KafkaListenerEndpoint}.
	 * <p>This create the necessary infrastructure to honor that endpoint
	 * with regards to its configuration.
	 * @param endpoint the endpoint to add
	 * @param factory the listener factory to use
	 * @see #registerListenerContainer(KafkaListenerEndpoint, KafkaListenerContainerFactory, boolean)
	 */
	public void registerListenerContainer(KafkaListenerEndpoint endpoint, KafkaListenerContainerFactory<?> factory) {
		registerListenerContainer(endpoint, factory, false);
	}

	/**
	 * Create a message listener container for the given {@link KafkaListenerEndpoint}.
	 * <p>This create the necessary infrastructure to honor that endpoint
	 * with regards to its configuration.
	 * <p>The {@code startImmediately} flag determines if the container should be
	 * started immediately.
	 * @param endpoint the endpoint to add.
	 * @param factory the {@link KafkaListenerContainerFactory} to use.
	 * @param startImmediately start the container immediately if necessary
	 * @see #getListenerContainers()
	 * @see #getListenerContainer(String)
	 */
	@SuppressWarnings("unchecked")
	public void registerListenerContainer(KafkaListenerEndpoint endpoint, KafkaListenerContainerFactory<?> factory,
			boolean startImmediately) {

		Assert.notNull(endpoint, "Endpoint must not be null");
		Assert.notNull(factory, "Factory must not be null");

		String id = endpoint.getId();
		Assert.hasText(id, "Endpoint id must not be empty");
		synchronized (this.listenerContainers) {
			Assert.state(!this.listenerContainers.containsKey(id),
					"Another endpoint is already registered with id '" + id + "'");
			MessageListenerContainer container = createListenerContainer(endpoint, factory);
			this.listenerContainers.put(id, container);
			ConfigurableApplicationContext appContext = this.applicationContext;
			String groupName = endpoint.getGroup();
			if (StringUtils.hasText(groupName) && appContext != null) {
				List<MessageListenerContainer> containerGroup;
				ContainerGroup group;
				if (appContext.containsBean(groupName)) { // NOSONAR - hasText
					containerGroup = appContext.getBean(groupName, List.class); // NOSONAR - hasText
					group = appContext.getBean(groupName + ".group", ContainerGroup.class);
				}
				else {
					containerGroup = new ArrayList<MessageListenerContainer>();
					appContext.getBeanFactory().registerSingleton(groupName, containerGroup); // NOSONAR - hasText
					group = new ContainerGroup(groupName);
					appContext.getBeanFactory().registerSingleton(groupName + ".group", group);
				}
				containerGroup.add(container);
				group.addContainers(container);
			}
			if (startImmediately) {
				startIfNecessary(container);
			}
		}
	}

	/**
	 * Unregister the listener container with the provided id.
	 * <p>
	 * IMPORTANT: this method simply removes the container from the registry. It does NOT
	 * call any {@link org.springframework.context.Lifecycle} or {@link DisposableBean}
	 * methods; you need to call them before or after calling this method to shut down the
	 * container.
	 * @param id the id.
	 * @return the container, if it was registered; null otherwise.
	 * @since 2.8.9
	 */
	@Nullable
	public MessageListenerContainer unregisterListenerContainer(String id) {
		return this.listenerContainers.remove(id);
	}

	/**
	 * Create and start a new {@link MessageListenerContainer} using the specified factory.
	 * @param endpoint the endpoint to create a {@link MessageListenerContainer}.
	 * @param factory the {@link KafkaListenerContainerFactory} to use.
	 * @return the {@link MessageListenerContainer}.
	 */
	protected MessageListenerContainer createListenerContainer(KafkaListenerEndpoint endpoint,
			KafkaListenerContainerFactory<?> factory) {

		if (endpoint instanceof MethodKafkaListenerEndpoint) {
			MethodKafkaListenerEndpoint<?, ?> mkle = (MethodKafkaListenerEndpoint<?, ?>) endpoint;
			Object bean = mkle.getBean();
			if (bean instanceof EndpointHandlerMethod) {
				EndpointHandlerMethod ehm = (EndpointHandlerMethod) bean;
				ehm = new EndpointHandlerMethod(ehm.resolveBean(this.applicationContext), ehm.getMethodName());
				mkle.setBean(ehm.resolveBean(this.applicationContext));
				mkle.setMethod(ehm.getMethod());
			}
		}
		MessageListenerContainer listenerContainer = factory.createListenerContainer(endpoint);

		if (listenerContainer instanceof InitializingBean) {
			try {
				((InitializingBean) listenerContainer).afterPropertiesSet();
			}
			catch (Exception ex) {
				throw new BeanInitializationException("Failed to initialize message listener container", ex);
			}
		}

		int containerPhase = listenerContainer.getPhase();
		if (listenerContainer.isAutoStartup() &&
				containerPhase != AbstractMessageListenerContainer.DEFAULT_PHASE) {  // a custom phase value
			if (this.phase != AbstractMessageListenerContainer.DEFAULT_PHASE && this.phase != containerPhase) {
				throw new IllegalStateException("Encountered phase mismatch between container "
						+ "factory definitions: " + this.phase + " vs " + containerPhase);
			}
			this.phase = listenerContainer.getPhase();
		}

		return listenerContainer;
	}


	@Override
	public void destroy() {
		for (MessageListenerContainer listenerContainer : getListenerContainers()) {
			listenerContainer.destroy();
		}
	}


	// Delegating implementation of SmartLifecycle

	@Override
	public int getPhase() {
		return this.phase;
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void start() {
		for (MessageListenerContainer listenerContainer : getListenerContainers()) {
			startIfNecessary(listenerContainer);
		}
		this.running = true;
	}

	@Override
	public void stop() {
		this.running = false;
		for (MessageListenerContainer listenerContainer : getListenerContainers()) {
			listenerContainer.stop();
		}
	}

	@Override
	public void stop(Runnable callback) {
		this.running = false;
		Collection<MessageListenerContainer> listenerContainersToStop = getListenerContainers();
		if (listenerContainersToStop.size() > 0) {
			AggregatingCallback aggregatingCallback = new AggregatingCallback(listenerContainersToStop.size(),
					callback);
			for (MessageListenerContainer listenerContainer : listenerContainersToStop) {
				if (listenerContainer.isRunning()) {
					listenerContainer.stop(aggregatingCallback);
				}
				else {
					aggregatingCallback.run();
				}
			}
		}
		else {
			callback.run();
		}
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}


	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		if (event.getApplicationContext().equals(this.applicationContext)) {
			this.contextRefreshed = true;
		}
	}

	/**
	 * Start the specified {@link MessageListenerContainer} if it should be started
	 * on startup.
	 * @param listenerContainer the listener container to start.
	 * @see MessageListenerContainer#isAutoStartup()
	 */
	private void startIfNecessary(MessageListenerContainer listenerContainer) {
		if ((this.contextRefreshed && this.alwaysStartAfterRefresh) || listenerContainer.isAutoStartup()) {
			listenerContainer.start();
		}
	}


	private static final class AggregatingCallback implements Runnable {

		private final AtomicInteger count;

		private final Runnable finishCallback;

		private AggregatingCallback(int count, Runnable finishCallback) {
			this.count = new AtomicInteger(count);
			this.finishCallback = finishCallback;
		}

		@Override
		public void run() {
			if (this.count.decrementAndGet() <= 0) {
				this.finishCallback.run();
			}
		}

	}

}
