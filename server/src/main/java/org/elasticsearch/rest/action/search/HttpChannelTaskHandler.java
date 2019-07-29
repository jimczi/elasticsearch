/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * This class keeps track of which tasks came in from which {@link HttpChannel}, by allowing to associate
 * an {@link HttpChannel} with a {@link TaskId}, and also removing the link once the task is complete.
 * Additionally, it accepts a consumer that gets called whenever an http channel gets closed, which
 * can be used to cancel the associated task when the underlying connection gets closed.
 */
final class HttpChannelTaskHandler {
    final Map<HttpChannel, CloseListener> httpChannels = ConcurrentCollections.newConcurrentMap();

    <Response extends ActionResponse> void execute(NodeClient client, HttpChannel httpChannel, ActionRequest request,
                                                   ActionType<Response> actionType, ActionListener<Response> listener) {
        CloseListener closeListener = httpChannels.computeIfAbsent(httpChannel, k -> new CloseListener(client, httpChannel, httpChannels::remove));
        TaskRegistry reg = closeListener.taskRegistry();
        Task task = client.executeLocally(actionType, request, new ActionListener<>() {
            @Override
            public void onResponse(Response response) {
                reg.unregister();
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                reg.unregister();
            }
         });
        TaskId taskId = new TaskId(client.getLocalNodeId(), task.getId());
        reg.register(taskId);

        //TODO test case where listener is registered, but no tasks have been added yet:
        // - connection gets closed, channel will be removed, no tasks will be cancelled

        //TODO check that no tasks are left behind through assertions at node close
    }

    static class CloseListener implements ActionListener<Void> {
        final NodeClient client;
        final HttpChannel channel;
        final Consumer<HttpChannel> onClose;
        final Set<TaskId> taskIds = new HashSet<>();
        final Set<TaskId> unregistered = new HashSet<>();

        CloseListener(NodeClient client, HttpChannel channel, Consumer<HttpChannel> onClose) {
            this.client = client;
            this.channel = channel;
            this.onClose = onClose;
            channel.addCloseListener(this);
        }

        TaskRegistry taskRegistry() {
            return new TaskRegistry() {
                TaskId taskId;
                boolean unregistered;

                @Override
                public void register(TaskId taskId) {
                    synchronized (CloseListener.this) {
                        if (unregistered == false) {
                            this.taskId = taskId;
                            taskIds.add(taskId);
                        }
                    }
                }

                @Override
                public void unregister() {
                    synchronized (CloseListener.this) {
                        if (taskId != null) {
                            taskIds.remove(taskId);
                        }
                        // mark the task in case register is called after unregister
                        unregistered = true;
                    }
                }
            };
        }

        @Override
        public synchronized void onResponse(Void aVoid) {
            onClose.accept(channel);
            for (TaskId previousTaskId : taskIds) {
                CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
                cancelTasksRequest.setTaskId(previousTaskId);
                //We don't wait for cancel tasks to come back. Task cancellation is just best effort.
                //Note that cancel tasks fails if the user sending the search request does not have the permissions to call it.
                client.admin().cluster().cancelTasks(cancelTasksRequest, ActionListener.wrap(r -> {}, e -> {}));
            }
        }

        @Override
        public void onFailure(Exception e) {
            onResponse(null);
        }
    }

    private interface TaskRegistry {
        void register(TaskId taskId);
        void unregister();
    }
}
