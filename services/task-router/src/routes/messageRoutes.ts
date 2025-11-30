import { Router } from "express";
import { MessageController } from "../controllers/MessageController";

export function createMessageRoutes(controller: MessageController): Router {
  const router = Router();
  router.post("/messages", (req, res) => controller.createMessage(req, res));
  return router;
}
