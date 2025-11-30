import { Router } from "express";
import { MessageController } from "../controllers/MessageController";

export function createMessageRoutes(
  messageController: MessageController
): Router {
  const router = Router();

  router.post("/messages", (req, res) =>
    messageController.createMessage(req, res)
  );

  return router;
}
