/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.builder.component.dsl;

import javax.annotation.Generated;
import org.apache.camel.Component;
import org.apache.camel.builder.component.AbstractComponentBuilder;
import org.apache.camel.builder.component.ComponentBuilder;
import org.apache.camel.component.crypto.cms.CryptoCmsComponent;

/**
 * Encrypt, decrypt, sign and verify data in CMS Enveloped Data format.
 * 
 * Generated by camel-package-maven-plugin - do not edit this file!
 */
@Generated("org.apache.camel.maven.packaging.ComponentDslMojo")
public interface CryptoCmsComponentBuilderFactory {

    /**
     * Crypto CMS (camel-crypto-cms)
     * Encrypt, decrypt, sign and verify data in CMS Enveloped Data format.
     * 
     * Category: security,transformation
     * Since: 2.20
     * Maven coordinates: org.apache.camel:camel-crypto-cms
     */
    @Deprecated
    static CryptoCmsComponentBuilder cryptoCms() {
        return new CryptoCmsComponentBuilderImpl();
    }

    /**
     * Builder for the Crypto CMS component.
     */
    interface CryptoCmsComponentBuilder
            extends
                ComponentBuilder<CryptoCmsComponent> {
        /**
         * Whether the producer should be started lazy (on the first message).
         * By starting lazy you can use this to allow CamelContext and routes to
         * startup in situations where a producer may otherwise fail during
         * starting and cause the route to fail being started. By deferring this
         * startup to be lazy then the startup failure can be handled during
         * routing messages via Camel's routing error handlers. Beware that when
         * the first message is processed then creating and starting the
         * producer may take a little time and prolong the total processing time
         * of the processing.
         * 
         * The option is a: <code>boolean</code> type.
         * 
         * Default: false
         * Group: producer
         */
        default CryptoCmsComponentBuilder lazyStartProducer(
                boolean lazyStartProducer) {
            doSetProperty("lazyStartProducer", lazyStartProducer);
            return this;
        }
        /**
         * Whether the component should use basic property binding (Camel 2.x)
         * or the newer property binding with additional capabilities.
         * 
         * The option is a: <code>boolean</code> type.
         * 
         * Default: false
         * Group: advanced
         */
        default CryptoCmsComponentBuilder basicPropertyBinding(
                boolean basicPropertyBinding) {
            doSetProperty("basicPropertyBinding", basicPropertyBinding);
            return this;
        }
        /**
         * To configure the shared EnvelopedDataDecryptorConfiguration, which
         * determines the uri parameters for the decrypt operation.
         * 
         * The option is a:
         * <code>org.apache.camel.component.crypto.cms.crypt.EnvelopedDataDecryptorConfiguration</code> type.
         * 
         * Group: advanced
         */
        default CryptoCmsComponentBuilder envelopedDataDecryptorConfiguration(
                org.apache.camel.component.crypto.cms.crypt.EnvelopedDataDecryptorConfiguration envelopedDataDecryptorConfiguration) {
            doSetProperty("envelopedDataDecryptorConfiguration", envelopedDataDecryptorConfiguration);
            return this;
        }
        /**
         * To configure the shared SignedDataVerifierConfiguration, which
         * determines the uri parameters for the verify operation.
         * 
         * The option is a:
         * <code>org.apache.camel.component.crypto.cms.sig.SignedDataVerifierConfiguration</code> type.
         * 
         * Group: advanced
         */
        default CryptoCmsComponentBuilder signedDataVerifierConfiguration(
                org.apache.camel.component.crypto.cms.sig.SignedDataVerifierConfiguration signedDataVerifierConfiguration) {
            doSetProperty("signedDataVerifierConfiguration", signedDataVerifierConfiguration);
            return this;
        }
    }

    class CryptoCmsComponentBuilderImpl
            extends
                AbstractComponentBuilder<CryptoCmsComponent>
            implements
                CryptoCmsComponentBuilder {
        @Override
        protected CryptoCmsComponent buildConcreteComponent() {
            return new CryptoCmsComponent();
        }
        @Override
        protected boolean setPropertyOnComponent(
                Component component,
                String name,
                Object value) {
            switch (name) {
            case "lazyStartProducer": ((CryptoCmsComponent) component).setLazyStartProducer((boolean) value); return true;
            case "basicPropertyBinding": ((CryptoCmsComponent) component).setBasicPropertyBinding((boolean) value); return true;
            case "envelopedDataDecryptorConfiguration": ((CryptoCmsComponent) component).setEnvelopedDataDecryptorConfiguration((org.apache.camel.component.crypto.cms.crypt.EnvelopedDataDecryptorConfiguration) value); return true;
            case "signedDataVerifierConfiguration": ((CryptoCmsComponent) component).setSignedDataVerifierConfiguration((org.apache.camel.component.crypto.cms.sig.SignedDataVerifierConfiguration) value); return true;
            default: return false;
            }
        }
    }
}