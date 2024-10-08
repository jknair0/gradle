/*
 * Copyright 2013 the original author or authors.
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

package org.gradle.ide.visualstudio.tasks.internal

import com.google.common.collect.Sets
import org.gradle.api.Action
import org.gradle.api.NonNullApi
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.Nested
import org.gradle.ide.visualstudio.TextProvider
import org.gradle.plugins.ide.internal.generator.AbstractPersistableConfigurationObject
import org.gradle.util.internal.TextUtil

import static org.gradle.ide.visualstudio.internal.DefaultVisualStudioProject.getUUID

@NonNullApi
class VisualStudioSolutionFile extends AbstractPersistableConfigurationObject {
    List<Action<? super TextProvider>> actions = new ArrayList<Action<? super TextProvider>>()
    private Map<File, String> projects = new LinkedHashMap<>()
    private Map<File, Set<ConfigurationSpec>> projectConfigurations = new LinkedHashMap<>()
    private baseText

    protected String getDefaultResourceName() {
        'default.sln'
    }

    void setProjects(List<ProjectSpec> projects) {
        projects.each { project ->
            this.projects[project.projectFile] = project.name
            def configs = this.projectConfigurations.computeIfAbsent(project.projectFile, f -> new HashSet<>())
            configs.addAll(project.configurations)
        }
    }

    @Override
    void load(InputStream inputStream) throws Exception {
        baseText = inputStream.text
    }

    @Override
    void store(OutputStream outputStream) {
        def provider = new SimpleTextProvider()
        generateContent(provider.asBuilder())
        actions.each {
            it.execute(provider)
        }
        outputStream << TextUtil.convertLineSeparators(provider.getText(), TextUtil.getWindowsLineSeparator())
    }

    private void generateContent(StringBuilder builder) {
        builder << baseText
        projects.each { File projectFile, String projectName ->
            builder << """
Project("{8BC9CEB8-8B4A-11D0-8D11-00A0C91BC942}") = "${projectName}", "${projectFile.absolutePath}", "${getUUID(projectFile)}"
EndProject"""
        }
        builder << """
Global
	GlobalSection(SolutionConfigurationPlatforms) = preSolution"""
        Set<String> configurationNames = Sets.newLinkedHashSet(projectConfigurations.values().flatten().collect({ it.name }).sort())
        configurationNames.each { String configurationName ->
            builder << """\n\t\t${configurationName} = ${configurationName}"""
        }
        builder << """
	EndGlobalSection
	GlobalSection(ProjectConfigurationPlatforms) = postSolution"""
        projects.each { File projectFile, String projectName ->
            def configurations = configurationNames.collect { String configurationName ->
                def result = []
                def configuration = projectConfigurations[projectFile].find({ configurationName == it.name })
                def lastConfiguration = projectConfigurations[projectFile].sort({ a, b -> a.name <=> b.name }).last()
                if (configuration == null) {
                    result.add("${configurationName}.ActiveCfg = ${lastConfiguration.name}")
                } else {
                    result.add("${configurationName}.ActiveCfg = ${configuration.name}")
                    if (configuration.buildable) {
                        result.add("${configurationName}.Build.0 = ${configuration.name}")
                    }
                }
                return result
            }

            configurations.flatten().sort().each { String configuration ->
                builder << "\n\t\t${getUUID(projectFile)}.${configuration}"
            }
        }


        builder << """
	EndGlobalSection
	GlobalSection(SolutionProperties) = preSolution
		HideSolutionNode = FALSE
	EndGlobalSection
EndGlobal
"""
    }

    static class SimpleTextProvider implements TextProvider {
        private final StringBuilder builder = new StringBuilder()

        StringBuilder asBuilder() {
            return builder
        }

        String getText() {
            return builder.toString()
        }

        void setText(String value) {
            builder.replace(0, builder.length(), value)
        }
    }

    static class ConfigurationSpec {
        @Input
        final String name
        @Input
        final boolean buildable

        ConfigurationSpec(String name, boolean buildable) {
            this.name = name
            this.buildable = buildable
        }
    }

    static class ProjectSpec {
        @Input
        final String name
        @Internal
        final File projectFile
        @Nested
        final List<ConfigurationSpec> configurations

        ProjectSpec(String name, File projectFile, List<ConfigurationSpec> configurations) {
            this.name = name
            this.projectFile = projectFile
            this.configurations = configurations
        }

        @Input
        String getProjectFilePath() {
            return projectFile.absolutePath
        }
    }
}
