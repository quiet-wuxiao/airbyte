package io.airbyte.integrations.standardtest.destination;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public interface Modifier {

//  default void modifyData(List<AirbyteMessage> messages, Map<String, String> datesField)
//      throws IOException {
//    for (AirbyteMessage message : messages) {
//      if (message.getType() == Type.RECORD) {
//        modify((ObjectNode) message.getRecord().getData(), datesField);
//      }
//    }
//  }

  default void modifyData(List<AirbyteMessage> messages, Map<String, String> datesField)
      throws IOException {
    final List<AirbyteMessage> messages2 = MoreResources.readResource(
            messagesFileName()).lines()
        .map(record -> Jsons.deserialize(record, AirbyteMessage.class)).toList();
    List<AirbyteMessage> mesToRemove = new ArrayList<>();
    for (AirbyteMessage message : messages) {
      if (message.getType() == Type.RECORD) {
        for (String key : datesField.keySet()) {
          if (message.getRecord().getData().has(key))
            mesToRemove.add(message);
        }
      }
    }
    messages.removeAll(mesToRemove);
    messages.addAll(messages2);
  }

  default void modify(ObjectNode data, Map<String, String> datesField) throws IOException {}

  default boolean shouldBeModified() {
    return false;
  }

  default String messagesFileName() {
    return StringUtils.EMPTY;
  }
}
