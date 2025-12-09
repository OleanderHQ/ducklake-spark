plugins {
    id("java-library")
    id("com.github.johnrengelman.shadow") version "8.1.1"
    id("io.freefair.lombok") version "8.10.2"
    id("com.diffplug.spotless") version "6.25.0"
}

group = "dev.oleander"
version = "0.1.0-SNAPSHOT"

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
