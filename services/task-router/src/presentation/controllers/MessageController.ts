import { Request, Response } from "express";
import { AcceptMessageUseCase } from "../../domain/usecases/AcceptMessageUseCase";
import { CreateMessageRequest } from "../../domain/entities/Message";

export class MessageController {
  constructor(private acceptMessageUseCase: AcceptMessageUseCase) {}

  async createMessage(req: Request, res: Response): Promise<void> {
    try {
      const request: CreateMessageRequest = req.body;
      const response = await this.acceptMessageUseCase.execute(request);

      if (response.status === "DUPLICATE") {
        res.status(200).json(response);
      } else {
        res.status(202).json(response);
      }
    } catch (error: any) {
      console.error("Error processing message:", error.message);
      res.status(400).json({ error: error.message });
    }
  }
}
