/*
 * Copyright 2024 the original author or authors.
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

package org.gradle.ide.sync

import org.gradle.test.fixtures.server.http.BlockingHttpServer
import org.gradle.util.internal.ToBeImplemented
import org.junit.Rule
import spock.lang.Ignore

@Ignore("https://github.com/gradle/gradle-private/issues/4438")
class IsolatedProjectsParallelSyncTest extends AbstractIdeaSyncTest {

    @Rule
    BlockingHttpServer server = new BlockingHttpServer(5_000)

    def setup() {
        server.start()
    }

    @ToBeImplemented
    def 'projects are configured in parallel during IDEA sync'() {
        given:
        simpleProject()
        server.expect("configure-root")
        server.expect("configure-a")
        server.expect("configure-b")
        // TODO: isolated expected behaviour for parallel sync
//        server.expect("configure-root")
//        server.expectConcurrent("configure-a", "configure-b")

        expect:
        ideaSync(IDEA_VERSION)
    }

    private void simpleProject() {
        file("settings.gradle") << """
            rootProject.name = 'project-under-test'
            include ':a'
            include ':b'
        """

        file("gradle.properties") << """
            org.gradle.configuration-cache.problems=warn
            org.gradle.unsafe.isolated-projects=true
        """

        file("build.gradle") << """
            ${server.callFromBuildUsingExpression("'configure-root'")}
        """

        file("a/build.gradle") << """
            ${server.callFromBuildUsingExpression("'configure-a'")}
        """

        file("b/build.gradle") << """
            ${server.callFromBuildUsingExpression("'configure-b'")}
        """
    }
}
