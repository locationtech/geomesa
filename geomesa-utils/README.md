# GeoMesa Utils

## Simple Feature Wrapper Generation
Tools can generate wrapper classes for simple feature types defined in TypeSafe Config files. Your
config files should be under src/main/resources. Add the following snippet to your pom, specifying
the package you would like the generated class to reside in:

    <build>
        ...
        <plugins>
            ...
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>exec-maven-plugin</artifactId>
                <version>1.3.2</version>
                <executions>
                    <execution>
                        <id>generate-sft-wrappers</id>
                        <phase>generate-sources</phase>
                        <goals>
                            <goal>java</goal>
                        </goals>
                        <configuration>
                            <mainClass>org.locationtech.geomesa.utils.geotools.GenerateFeatureWrappers</mainClass>
                            <cleanupDaemonThreads>false</cleanupDaemonThreads>
                            <killAfter>-1</killAfter>
                            <arguments>
                                <argument>${project.basedir}</argument>
                                <argument>org.foo.mypackage</argument>
                            </arguments>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
