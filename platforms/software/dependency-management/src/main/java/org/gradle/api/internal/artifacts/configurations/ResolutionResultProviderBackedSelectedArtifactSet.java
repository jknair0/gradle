/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradle.api.internal.artifacts.configurations;

import org.gradle.api.artifacts.Configuration;
import org.gradle.api.internal.artifacts.ivyservice.resolveengine.artifact.ArtifactVisitor;
import org.gradle.api.internal.artifacts.ivyservice.resolveengine.artifact.SelectedArtifactSet;
import org.gradle.api.internal.tasks.TaskDependencyResolveContext;

/**
 * A {@link SelectedArtifactSet} that is backed by a {@link ResolutionResultProvider}.
 * <p>
 * This is used as a source for constructing {@link ResolutionBackedFileCollection}
 * instances derived from resolving a {@link Configuration}.
 */
public class ResolutionResultProviderBackedSelectedArtifactSet implements SelectedArtifactSet {

    private final ResolutionResultProvider<SelectedArtifactSet> artifacts;

    private SelectedArtifactSet delegate;

    public ResolutionResultProviderBackedSelectedArtifactSet(
        ResolutionResultProvider<SelectedArtifactSet> artifacts
    ) {
        this.artifacts = artifacts;
    }

    @Override
    public void visitDependencies(TaskDependencyResolveContext context) {
        artifacts.getTaskDependencyValue().visitDependencies(context);
    }

    @Override
    public void visitArtifacts(ArtifactVisitor visitor, boolean continueOnSelectionFailure) {
        if (delegate == null) {
            delegate = artifacts.getValue();
        }
        delegate.visitArtifacts(visitor, continueOnSelectionFailure);
    }
}
