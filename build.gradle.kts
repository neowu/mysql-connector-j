plugins {
    java
    `java-library`
    `maven-publish`
}

group = "core.framework.mysql"
version = "8.4.0-r4"

repositories {
    mavenCentral()
}

val mavenURL = "/Users/neo/depot/maven-repo"

sourceSets {
    main {
        java {
            srcDirs("src/main/core-api/java")
            srcDirs("src/main/core-impl/java")
            srcDirs("src/main/protocol-impl/java")
            srcDirs("src/main/user-api/java")
            srcDirs("src/main/user-impl/java")
        }
    }
}

dependencies {
    compileOnly("org.slf4j:slf4j-api:2.0.9")
    compileOnly(project(":api"))
}

allprojects {
    apply(plugin = "java")
    java {
        withSourcesJar()
    }
    layout.buildDirectory.set(file("$rootDir/build/${rootDir.toPath().relativize(projectDir.toPath())}"))
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
        }
    }
    repositories {
        maven { url = uri(mavenURL) }
    }
}
