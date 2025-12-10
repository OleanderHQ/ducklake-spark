plugins {
    id("java-library")
    id("com.github.johnrengelman.shadow") version "8.1.1"
    id("io.freefair.lombok") version "8.10.2"
    id("com.diffplug.spotless") version "6.25.0"
    id("maven-publish")
    id("signing")
    id("io.github.gradle-nexus.publish-plugin") version "2.0.0"
}

group = "dev.oleander"
version = "0.1.1-SNAPSHOT"
description = "SparkCatalog implementation for Ducklake open table format"

val scalaBinary: String = providers.gradleProperty("scalaBinary").orElse("2.12").get()
val sparkVersion: String = providers.gradleProperty("sparkVersion").orElse("3.5.5").get()

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.apache.spark:spark-sql_${scalaBinary}:${sparkVersion}")

    implementation("org.jdbi:jdbi3-core:3.45.0")
    implementation("org.jdbi:jdbi3-sqlobject:3.45.0")
    implementation("org.jdbi:jdbi3-postgres:3.45.0")
    implementation("org.jdbi:jdbi3-jackson2:3.45.0")
    implementation("org.postgresql:postgresql:42.7.4")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    withSourcesJar()
    withJavadocJar()
}

spotless {
    java {
        target("src/**/*.java")

        importOrder("jakarta", "java", "javax", "org", "com", "")
        removeUnusedImports()

        eclipse().configFile("spotless/eclipse-java-formatter.xml")
    }

    format("misc") {
        target("*.md", ".gitignore", ".gitattributes")
        trimTrailingWhitespace()
        endWithNewline()
        indentWithSpaces(2)
    }
}

tasks.test {
    useJUnitPlatform()
}

tasks.jar {
    archiveClassifier.set("unshaded")
}

tasks.shadowJar {
    archiveClassifier.set("")
    relocate("org.postgresql", "dev.oleander.spark.postgresql.relocated.org.postgresql")
    relocate("org.jdbi", "dev.oleander.spark.postgresql.relocated.org.jdbi")
    mergeServiceFiles {
        include("META-INF/services/*")
    }
}

tasks.assemble {
    dependsOn(tasks.shadowJar)
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifact(tasks.shadowJar)
            artifact(tasks.named<Jar>("sourcesJar"))
            artifact(tasks.named<Jar>("javadocJar"))

            pom {
                name.set("ducklake-spark")
                description.set(project.description)
                url.set("https://github.com/OleanderHQ/ducklake-spark")

                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }

                developers {
                    developer {
                        id.set("oleander")
                        name.set("Oleander Team")
                    }
                }

                scm {
                    url.set("https://github.com/OleanderHQ/ducklake-spark")
                    connection.set("scm:git:https://github.com/OleanderHQ/ducklake-spark.git")
                    developerConnection.set("scm:git:git@github.com:OleanderHQ/ducklake-spark.git")
                }
            }
        }
    }
}

signing {
    useInMemoryPgpKeys(
        System.getenv("PUBLISH_SIGNING_KEY"),
        System.getenv("PUBLISH_SIGNING_PASSWORD")
    )
    sign(publishing.publications["mavenJava"])
}

nexusPublishing {
    repositories {
        sonatype {
            nexusUrl.set(uri("https://ossrh-staging-api.central.sonatype.com/service/local/"))
            snapshotRepositoryUrl.set(uri("https://central.sonatype.com/repository/maven-snapshots/"))
            username.set(System.getenv("SONATYPE_USERNAME"))
            password.set(System.getenv("SONATYPE_PASSWORD"))
        }
    }
}
