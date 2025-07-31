/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { useEffect, useState } from "react";
import { Heading, Text, HStack, Spinner, Alert } from "@chakra-ui/react";
import { LuSettings } from "react-icons/lu";
import { Dialog } from "src/components/ui";
import { useConfigServiceGetBackendsOrderValue } from "openapi/queries";

type BackendOrderModalProps = {
  onClose: () => void;
  open: boolean;
};


export const BackendOrderModal: React.FC<BackendOrderModalProps> = ({ onClose, open }) => {
  const [backendOrder, setBackendOrder] = useState<null>();
  const { data, error } = useConfigServiceGetBackendsOrderValue()

  const onOpenChange = () => {
    onClose();
  };

  useEffect(() => {
    if (!open) {return;}
    setBackendOrder(data.sections[0].options[0].value)
  }, [open]);

  return (
    <Dialog.Root onOpenChange={onOpenChange} open={open} scrollBehavior="inside" size="md">
      <Dialog.Content backdrop p={4}>
        <Dialog.Header display="flex" justifyContent="space-between">
          <HStack fontSize="xl">
            <LuSettings />
            <Heading size="md">Backend Order</Heading>
          </HStack>
        </Dialog.Header>

        <Dialog.CloseTrigger />

        <Dialog.Body>
          {loading ? <Spinner /> : null}
          {error ? <Alert mt={2} status="error">
              {error}
            </Alert> : null}
          {!loading && !error && backendOrder ? <Text fontFamily="mono" fontSize="sm" whiteSpace="pre-wrap">
              {backendOrder}
            </Text> : null}
        </Dialog.Body>
      </Dialog.Content>
    </Dialog.Root>
  );
};
